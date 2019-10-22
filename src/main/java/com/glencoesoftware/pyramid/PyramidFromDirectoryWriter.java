/**
 * Copyright (c) 2019 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.pyramid;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import loci.common.DataTools;
import loci.common.DebugTools;
import loci.common.Region;
import loci.common.image.IImageScaler;
import loci.common.image.SimpleImageScaler;
import loci.common.services.DependencyException;
import loci.common.services.ServiceFactory;
import loci.formats.ClassList;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.IFormatReader;
import loci.formats.ImageReader;
import loci.formats.MetadataTools;
import loci.formats.in.APNGReader;
import loci.formats.in.DefaultMetadataOptions;
import loci.formats.in.JPEGReader;
import loci.formats.in.MetadataOptions;
import loci.formats.in.MinimalTiffReader;
import loci.formats.ome.OMEPyramidStore;
import loci.formats.out.PyramidOMETiffWriter;
import loci.formats.services.OMEXMLService;
import loci.formats.tiff.IFD;

import ome.xml.model.primitives.PositiveInteger;

import org.json.JSONObject;

import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Writes a pyramid OME-TIFF file using Bio-Formats 6.x.
 * Image tiles are read from files within a specific folder structure:
 *
 * root_folder/resolution_index/x_coordinate/y_coordinate.tiff
 *
 * The resulting OME-TIFF file can be read using Bio-Formats 6.x,
 * for example with this command:
 *
 * $ showinf -noflat pyramid.ome.tiff -nopix
 *
 * which should show the expected number of resolutions with the
 * expected image dimensions.
 *
 * Missing tiles are supported, but every resolution and X coordinate
 * must have at least one valid tile file.
 * Any missing tiles are filled in with pixel values of 0 (black).
 */
public class PyramidFromDirectoryWriter implements Callable<Void> {

    /** Name of label image file */
    private static final String LABEL_FILE = "LABELIMAGE.jpg";

    /** Name of macro image file */
    private static final String MACRO_FILE = "MACROIMAGE.jpg";

    /** Name of JSON metadata file */
    private static final String METADATA_FILE = "METADATA.json";

    /** Logger */
    private static final Logger log =
        LoggerFactory.getLogger(PyramidFromDirectoryWriter.class);

    /** Image writer */
    PyramidOMETiffWriter writer;

    /** Where to write? */
    @Option(
        names = "--output",
        arity = "1",
        required = true,
        description = "Relative path to the output OME-TIFF file"
    )
    String outputFilePath;

    /** Where to read? */
    @Parameters(
        index = "0",
        arity = "1",
        description = "Directory containing pixel data to convert"
    )
    String inputDirectory;

    @Option(
        names = "--debug",
        description = "Turn on debug logging"
    )
    boolean debug = false;

    @Option(
        names = "--compression",
        description = "Compression type for output OME-TIFF file (default: LZW)"
    )
    String compression = "LZW";

    /** FormatTools pixel type */
    Integer pixelType;

    /** Number of RGB channels */
    int rgbChannels;

    /** Writer metadata */
    OMEPyramidStore metadata;

    /** Number of resolutions */
    Integer numberOfResolutions;

    /** Endianness */
    Boolean littleEndian;

    Boolean interleaved;

    /** Store resolution info */
    List<ResolutionDescriptor> resolutions;

    boolean generateResolutions = false;

    private class ResolutionDescriptor {

        /** Resolution index (0 = the original image) */
        Integer resolutionNumber;

        /** Image width at this resolution */
        Integer sizeX;

        /** Image height at this resolution */
        Integer sizeY;

        /** Tile width at this resolution */
        Integer tileSizeX;

        /** Tile height at this resolution */
        Integer tileSizeY;

        /** Number of tiles along X axis */
        Integer numberOfTilesX;

        /** Number of tiles along Y axis */
        Integer numberOfTilesY;

        /** Absolute paths to each tile, indexed by X and Y */
        String[][] tileFiles;
    }

    /** Reader used for opening tile files. */
    private IFormatReader helperReader = null;

    public PyramidFromDirectoryWriter() {
        // specify a minimal list of readers to speed up tile reading
        ClassList<IFormatReader> validReaders =
            new ClassList<IFormatReader>(IFormatReader.class);
        validReaders.addClass(MinimalTiffReader.class);
        validReaders.addClass(JPEGReader.class);
        validReaders.addClass(APNGReader.class);
        helperReader = new ImageReader(validReaders);
    }

    public static void main(String[] args) {
        CommandLine.call(new PyramidFromDirectoryWriter(), args);
    }

    @Override
    public Void call() {
        if (!debug) {
            DebugTools.setRootLevel("INFO");
        }
        try {
            initialize();
            convertToPyramid();
        }
        catch (FormatException|IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    //* Initialization */

    /**
     * Get the file path to the upper-left-most tile in the given resolution.
     * This makes it easier to get a tile for reading metadata without
     * assuming that the (0, 0) tile exists.
     * Every resolution is still assumed to have at least one valid tile file.
     *
     * @param resolution the pyramid level, indexed from 0 (largest)
     * @return the absolute file path to the first tile file in the resolution
     */
    private String getFirstTileFile(int resolution) {
      if (generateResolutions && resolution > 0) {
          return getFirstTileFile(0);
      }
      File directory =
          new File(this.inputDirectory, String.valueOf(resolution));
      String[] x = directory.list();
      sortFiles(x);
      directory = new File(directory, x[0]);
      String[] y = directory.list();
      sortFiles(y);
      return new File(directory, y[0]).getAbsolutePath();
    }

    /**
     * Fill in the paths to all tile files for the given resolution.
     *
     * @param descriptor the ResolutionDescriptor representing a resolution
     */
    private void findTileFiles(ResolutionDescriptor descriptor) {
        if (!generateResolutions || descriptor.resolutionNumber == 0) {
            File directory = new File(this.inputDirectory,
                String.valueOf(descriptor.resolutionNumber));
            String[] x = directory.list();
            sortFiles(x);
            for (int xx=0; xx<x.length; xx++) {
                File xPath = new File(directory, x[xx]);
                String[] y = xPath.list();
                sortFiles(y);
                for (int yy=0; yy<y.length; yy++) {
                    descriptor.tileFiles[xx][yy] =
                        new File(xPath, y[yy]).getAbsolutePath();
                }
            }
        }
    }

    /**
     * Get the label image file, which may or may not exist.
     *
     * @return File representing the expected label image file
     */
    private File getLabelFile() {
      return Paths.get(this.inputDirectory, LABEL_FILE).toFile();
    }

    /**
     * Get the macro image file, which may or may not exist.
     *
     * @return File representing the expected macro image file
     */
    private File getMacroFile() {
      return Paths.get(this.inputDirectory, MACRO_FILE).toFile();
    }

    /**
     * Get the metadata image file, which may or may not exist.
     *
     * @return File representing the expected metadata image file
     */
    private File getMetadataFile() {
      return Paths.get(this.inputDirectory, METADATA_FILE).toFile();
    }

    /**
     * Provide tile from the input folder
     *
     * @param resolution the pyramid level, indexed from 0 (largest)
     * @param x the tile X index, from 0 to numberOfTilesX
     * @param y the tile Y index, from 0 to numberOfTilesY
     * @param region specifies the width and height to read;
                     if null, the whole tile is read
     * @return byte array containing the pixels for the tile
     */
    private byte[] getInputTileBytes(int resolution,
        int x, int y, Region region)
        throws FormatException, IOException
    {
        ResolutionDescriptor descriptor = resolutions.get(resolution);
        int xy = descriptor.tileSizeX * descriptor.tileSizeY;
        if (region != null) {
            xy = region.width * region.height;
        }
        int bpp = FormatTools.getBytesPerPixel(pixelType);

        String path = descriptor.tileFiles[x][y];
        if (path == null) {
            return null;
        }
        if (!new File(path).exists()) {
            return new byte[xy * bpp * rgbChannels];
        }
        try {
            helperReader.setId(path);
            if (region == null) {
                return helperReader.openBytes(0);
            }
            return helperReader.openBytes(0, 0, 0, region.width, region.height);
        }
        finally {
            helperReader.close();
        }
    }

    /**
     * Calculate image width and height for each resolution.
     * Uses the first tile in the resolution to find the tile size,
     * and {@link #findNumberOfTiles(int)} to find the number of tiles
     * in X and Y.
     */
    public void describePyramid() throws FormatException, IOException {
        log.info("Number of resolution levels: {}", numberOfResolutions);
        resolutions = new ArrayList<ResolutionDescriptor>();
        for (
            int resolution = 0; resolution < numberOfResolutions; resolution++)
        {
            ResolutionDescriptor descriptor = new ResolutionDescriptor();
            String file = getFirstTileFile(resolution);
            helperReader.setId(file);
            descriptor.resolutionNumber = resolution;
            descriptor.tileSizeX = helperReader.getSizeX();
            descriptor.tileSizeY = helperReader.getSizeY();
            int[] tileCount = findNumberOfTiles(resolution);
            descriptor.numberOfTilesX = tileCount[0];
            descriptor.numberOfTilesY = tileCount[1];
            if (resolution == 0) {
                descriptor.sizeX =
                  descriptor.tileSizeX * descriptor.numberOfTilesX;
                descriptor.sizeY =
                  descriptor.tileSizeY * descriptor.numberOfTilesY;
            }
            else {
                descriptor.sizeX = resolutions.get(resolution - 1).sizeX / 2;
                descriptor.sizeY = resolutions.get(resolution - 1).sizeY / 2;
            }
            descriptor.tileFiles =
              new String[descriptor.numberOfTilesX][descriptor.numberOfTilesY];
            findTileFiles(descriptor);
            log.info("Resolution: {}; Size: [{}, {}]; " +
                      "Grid size: [{}, {}]",
                      resolution, descriptor.sizeX, descriptor.sizeY,
                      descriptor.numberOfTilesX, descriptor.numberOfTilesY);
            resolutions.add(descriptor);

            // subresolutions will be generated from the full resolution
            if (resolution == 0 && numberOfResolutions == 1) {
                generateResolutions = true;
                while (descriptor.sizeX / getScale(numberOfResolutions) >=
                    descriptor.tileSizeX)
                {
                    numberOfResolutions++;
                }
            }
        }
    }

    /**
     * Calculate the number of resolutions based upon the number
     * of directories in the base input directory.
     */
    private void findNumberOfResolutions() {
        File rootDirectory = new File(this.inputDirectory);
        numberOfResolutions = rootDirectory.list(
            (dir, name) -> new File(dir, name).isDirectory()).length;
    }

    private double getScale(int resolution) {
        return Math.pow(2, resolution);
    }

    /**
     * Return number of tiles in X, Y for a given resolution
     *
     * @param resolution pyramid resolution index, starting from 0
     * @return int array of length 2, containing the number of tiles in X and Y
     */
    private int[] findNumberOfTiles(int resolution) {
        if (generateResolutions && resolution > 0) {
            int[] tiles = findNumberOfTiles(0);
            double scale = getScale(resolution);
            tiles[0] = (int) Math.ceil(tiles[0] / scale);
            tiles[1] = (int) Math.ceil(tiles[1] / scale);
            return tiles;
        }
        /*
        Find number of subdirs for X
        Find number of files in subdirs for Y
        */
        File rootDirectory = new File(this.inputDirectory);

        // only list directories, ignoring label/macro/metadata files
        String[] resolutionDirectories =
          rootDirectory.list((dir, name) -> new File(dir, name).isDirectory());
        sortFiles(resolutionDirectories);
        File resolutionDirectory =
          new File(rootDirectory, resolutionDirectories[resolution]);
        String[] xDirectories = resolutionDirectory.list();
        int tilesX = xDirectories.length;
        String[] yDirectories =
          new File(resolutionDirectory, xDirectories[0]).list();
        int tilesY = yDirectories.length;
        return new int[] {tilesX, tilesY};
    }

    /**
     * Populate number of channels, pixels types, endianess, etc.
     * based on the first tile
     * @throws IOException
     * @throws FormatException
     */
    private void populateMetadataFromInputFile()
            throws FormatException, IOException
    {
        this.findNumberOfResolutions();
        helperReader.setOriginalMetadataPopulated(true);
        MetadataOptions options = new DefaultMetadataOptions();
        options.setValidate(true);
        helperReader.setMetadataOptions(options);
        // Read metadata from the first tile on the grid
        helperReader.setId(this.getFirstTileFile(0));
        this.pixelType = helperReader.getPixelType();
        rgbChannels = helperReader.getRGBChannelCount();
        this.littleEndian = helperReader.isLittleEndian();
        interleaved = helperReader.isInterleaved();
    }

    /**
     * Set up the TIFF writer with all necessary metadata.
     * After this method is called, image data can be written.
     */
    public void initialize() throws FormatException, IOException {
        log.info("Creating tiled pyramid file {}", this.outputFilePath);
        populateMetadataFromInputFile();
        describePyramid();
        metadata = (OMEPyramidStore) MetadataTools.createOMEXMLMetadata();

        File metadataFile = getMetadataFile();
        Hashtable<String, Object> originalMeta =
            new Hashtable<String, Object>();
        if (metadataFile != null && metadataFile.exists()) {
            String jsonMetadata =
                DataTools.readFile(metadataFile.getAbsolutePath());
            JSONObject json = new JSONObject(jsonMetadata);

            parseJSONValues(json, originalMeta, "");

            try {
                ServiceFactory factory = new ServiceFactory();
                OMEXMLService service =
                    factory.getInstance(OMEXMLService.class);
                service.populateOriginalMetadata(metadata, originalMeta);
            }
            catch (DependencyException e) {
                log.warn("Could not attach metadata annotations", e);
            }
        }

        for (ResolutionDescriptor descriptor : resolutions) {
            log.info("Adding metadata for resolution: {}",
                descriptor.resolutionNumber);

            String levelKey =
                "Image #0 | Level sizes #" + descriptor.resolutionNumber;
            String realX = originalMeta.get(levelKey + " | X").toString();
            String realY = originalMeta.get(levelKey + " | Y").toString();
            if (realX != null) {
                descriptor.sizeX = DataTools.parseDouble(realX).intValue();
            }
            if (realY != null) {
                descriptor.sizeY = DataTools.parseDouble(realY).intValue();
            }

            if (descriptor.resolutionNumber == 0) {
                MetadataTools.populateMetadata(
                    this.metadata, 0, null, this.littleEndian, "XYCZT",
                    FormatTools.getPixelTypeString(this.pixelType),
                    descriptor.sizeX, descriptor.sizeY, 1,
                    rgbChannels, 1, rgbChannels);
            }
            else {
                metadata.setResolutionSizeX(new PositiveInteger(
                    descriptor.sizeX), 0, descriptor.resolutionNumber);
                metadata.setResolutionSizeY(new PositiveInteger(
                    descriptor.sizeY), 0, descriptor.resolutionNumber);
            }
        }

        File label = getLabelFile();
        File macro = getMacroFile();

        int nextImage = 1;
        if (label != null && label.exists()) {
            try {
                helperReader.setId(label.getAbsolutePath());
                MetadataTools.populateMetadata(metadata, nextImage,
                    "Label", helperReader.getCoreMetadataList().get(0));
                nextImage++;
            }
            finally {
                helperReader.close();
            }
        }
        if (macro != null && macro.exists()) {
            try {
                helperReader.setId(macro.getAbsolutePath());
                MetadataTools.populateMetadata(metadata, nextImage,
                    "Macro", helperReader.getCoreMetadataList().get(0));
            }
            finally {
                helperReader.close();
            }
        }

        writer = new PyramidOMETiffWriter();
        writer.setBigTiff(true);
        writer.setWriteSequentially(true);
        writer.setMetadataRetrieve(this.metadata);
        if (compression != null) {
            writer.setCompression(compression);
        }
        writer.setInterleaved(interleaved);
        writer.setId(this.outputFilePath);
    }

    private void parseJSONValues(JSONObject root,
        Hashtable<String, Object> originalMeta, String prefix)
    {
        for (String key : root.keySet()) {
            Object value = root.get(key);

            if (value instanceof JSONObject) {
                parseJSONValues(
                    (JSONObject) value, originalMeta,
                    prefix.isEmpty() ? key : prefix + " | " + key);
            }
            else {
                originalMeta.put(prefix.isEmpty() ? key : prefix + " | " + key,
                    value.toString());
            }
        }
    }

     //* Conversion */

    /**
     * Writes all image data to the initialized TIFF writer
     */
    public void convertToPyramid() throws FormatException, IOException {
        // convert every resolution in the pyramid
        for (int resolution=0; resolution<numberOfResolutions; resolution++) {
            log.info("Converting resolution #{}", resolution);
            ResolutionDescriptor descriptor = resolutions.get(resolution);
            writer.setResolution(resolution);
            IFD ifd = new IFD();
            if (!generateResolutions || resolution == 0) {
                // if the resolution has already been calculated,
                // just read each tile from disk and store in the OME-TIFF
                ifd.put(IFD.TILE_WIDTH, descriptor.tileSizeX);
                ifd.put(IFD.TILE_LENGTH, descriptor.tileSizeY);
                Region region = new Region(0, 0, 0, 0);
                for (int y = 0; y < descriptor.numberOfTilesY; y ++) {
                    region.y = y * descriptor.tileSizeY;
                    region.height = (int) Math.min(
                        descriptor.tileSizeY, descriptor.sizeY - region.y);
                    for (int x = 0; x < descriptor.numberOfTilesX; x++) {
                        region.x = x * descriptor.tileSizeX;
                        region.width = (int) Math.min(
                            descriptor.tileSizeX, descriptor.sizeX - region.x);
                        byte[] tileBytes =
                            getInputTileBytes(resolution, x, y, region);
                        writeTile(0, tileBytes, region, ifd);
                    }
                }
            }
            else {
                // if the resolution needs to be calculated,
                // read one full resolution at a time and downsample before
                // storing in the OME-TIFF
                // this means that tiles in the OME-TIFF will be progressively
                // smaller, e.g. 512x512 at full resolution will become
                // 16x16 at resolution 5
                // this is much simpler than trying to repack multiple
                // downsampled tiles into the same size tile for all resolutions
                IImageScaler scaler = new SimpleImageScaler();
                ResolutionDescriptor zero = resolutions.get(0);
                int scale = (int) getScale(resolution);
                ifd.put(IFD.TILE_WIDTH, descriptor.tileSizeX / scale);
                ifd.put(IFD.TILE_LENGTH, descriptor.tileSizeY / scale);

                int bpp = FormatTools.getBytesPerPixel(pixelType);
                Region region = new Region();
                int thisTileWidth = zero.tileSizeX / scale;
                int thisTileHeight = zero.tileSizeY / scale;
                for (int y=0; y<zero.numberOfTilesY; y++) {
                    region.y = y * thisTileHeight;
                    region.height = (int) Math.min(
                        thisTileHeight, descriptor.sizeY - region.y);
                    for (int x=0; x<zero.numberOfTilesX; x++) {
                        region.x = x * thisTileWidth;
                        region.width = (int) Math.min(
                            thisTileWidth, descriptor.sizeX - region.x);
                        byte[] fullTile = getInputTileBytes(0, x, y, null);
                        byte[] downsampled = scaler.downsample(fullTile,
                            zero.tileSizeX, zero.tileSizeY,
                            scale, bpp, littleEndian,
                            FormatTools.isFloatingPoint(pixelType),
                            rgbChannels, interleaved);
                        writeTile(0, downsampled, region, ifd);
                    }
                }
            }
        }
        writer.setResolution(0);

        // add the label image, if present
        File label = getLabelFile();
        int nextImage = 1;
        if (label != null && label.exists()) {
            writer.setSeries(nextImage);
            try {
                helperReader.setId(label.getAbsolutePath());
                writer.saveBytes(0, helperReader.openBytes(0));
            }
            finally {
                helperReader.close();
            }
            nextImage++;
        }

        // add the macro image, if present
        File macro = getMacroFile();
        if (macro != null && macro.exists()) {
            writer.setSeries(nextImage);
            try {
                helperReader.setId(macro.getAbsolutePath());
                writer.saveBytes(0, helperReader.openBytes(0));
            }
            finally {
                helperReader.close();
            }
        }

        this.writer.close();
    }

    /**
     * Write a single tile to the writer's current resolution.
     *
     * @param imageNumber the plane number in the resolution
     * @param buffer the array containing the tile's pixel data
     * @param region the tile boundaries
     */
    private void writeTile(
            Integer imageNumber, byte[] buffer, Region region, IFD ifd)
                    throws FormatException, IOException
    {
        log.debug("Writing image: {}, region: {}", imageNumber, region);
        this.writer.saveBytes(imageNumber, buffer, ifd,
            region.x, region.y, region.width, region.height);
    }

    /**
     * Sort a list of files names numerically,
     * ignoring the .tiff extension if present.
     */
    private void sortFiles(String[] list) {
      Arrays.sort(list, Comparator.comparingInt(
        v -> Integer.parseInt(v.replaceAll(".tiff", ""))));
    }

}
