/**
 * Copyright (c) 2019 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.pyramid;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.FloatArrayDataBlock;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.zarr.N5ZarrReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import loci.common.DataTools;
import loci.common.DebugTools;
import loci.common.RandomAccessInputStream;
import loci.common.RandomAccessOutputStream;
import loci.common.Region;
import loci.common.image.IImageScaler;
import loci.common.image.SimpleImageScaler;
import loci.common.services.DependencyException;
import loci.common.services.ServiceException;
import loci.common.services.ServiceFactory;
import loci.formats.ClassList;
import loci.formats.FormatException;
import loci.formats.FormatHandler;
import loci.formats.FormatTools;
import loci.formats.IFormatReader;
import loci.formats.ImageReader;
import loci.formats.MetadataTools;
import loci.formats.in.APNGReader;
import loci.formats.in.BMPReader;
import loci.formats.in.DefaultMetadataOptions;
import loci.formats.in.JPEGReader;
import loci.formats.in.MetadataOptions;
import loci.formats.in.MinimalTiffReader;
import loci.formats.ome.OMEPyramidStore;
import loci.formats.out.PyramidOMETiffWriter;
import loci.formats.out.TiffWriter;
import loci.formats.services.OMEXMLService;
import loci.formats.tiff.IFD;
import loci.formats.tiff.TiffSaver;

import ome.xml.model.primitives.PositiveInteger;

import org.json.JSONObject;
import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;

import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Writes a pyramid OME-TIFF file or Bio-Formats 5.9.x "Faas" TIFF file.
 * Image tiles are read from files within a specific folder structure:
 *
 * root_folder/resolution_index/x_coordinate/y_coordinate.tiff
 *
 * root_folder/resolution_index/x_coordinate/y_coordinate/c-z-t.tiff
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

    static class CompressionTypes extends ArrayList<String> {
        CompressionTypes() {
            super(CompressionTypes.getCompressionTypes());
        }

        private static List<String> getCompressionTypes() {
            try (TiffWriter v = new TiffWriter()) {
                return Arrays.asList(v.getCompressionTypes());
            } catch (Exception e) {
                return new ArrayList<String>();
            }
        }
    }

    /** Scaling factor between two adjacent resolutions */
    private static final int PYRAMID_SCALE = 2;

    /** Name of label image file */
    private static final String LABEL_FILE = "LABELIMAGE.jpg";

    /** Name of macro image file */
    private static final String MACRO_FILE = "MACROIMAGE.jpg";

    /** Name of JSON metadata file */
    private static final String METADATA_FILE = "METADATA.json";

    /** Name of OME-XML metadata file */
    private static final String OMEXML_FILE = "METADATA.ome.xml";

    /** Logger */
    private static final Logger log =
        LoggerFactory.getLogger(PyramidFromDirectoryWriter.class);

    /** Image writer */
    TiffWriter writer;

    /** Where to write? */
    @Option(
        names = "--output",
        arity = "1",
        required = true,
        description = "Relative path to the output OME-TIFF file"
    )
    Path outputFilePath;

    /** Where to read? */
    @Parameters(
        index = "0",
        arity = "1",
        description = "Directory containing pixel data to convert"
    )
    Path inputDirectory;

    @Option(
        names = "--debug",
        description = "Turn on debug logging"
    )
    boolean debug = false;

    @Option(
        names = "--compression",
        completionCandidates = CompressionTypes.class,
        description = "Compression type for output OME-TIFF file " +
                      "(${COMPLETION-CANDIDATES}; default: ${DEFAULT-VALUE})"
    )
    String compression = "LZW";

    @Option(
        names = "--legacy",
        description = "Write a Bio-Formats 5.9.x pyramid instead of OME-TIFF"
    )
    boolean legacy = false;

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

    int planeCount = 1;
    int z = 1;
    int c = 1;
    int t = 1;

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
        Path[][][] tileFiles;
    }

    /** Reader used for opening tile files. */
    private IFormatReader helperReader = null;

    private N5FSReader n5Reader = null;

    public PyramidFromDirectoryWriter() {
        // specify a minimal list of readers to speed up tile reading
        ClassList<IFormatReader> validReaders =
            new ClassList<IFormatReader>(IFormatReader.class);
        validReaders.addClass(BMPReader.class);
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
            StopWatch t0 = new Slf4JStopWatch("initialize");
            try {
                initialize();
            }
            finally {
                t0.stop();
            }
            t0 = new Slf4JStopWatch("convertToPyramid");
            try {
                convertToPyramid();
            }
            finally {
                t0.stop();
            }
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
    private Path getFirstTileFile(int resolution) throws IOException {
      if (generateResolutions && resolution > 0) {
          return getFirstTileFile(0);
      }
      Path directory = inputDirectory.resolve(String.valueOf(resolution));
      List<Path> x = Files.list(directory).collect(Collectors.toList());
      sortFiles(x);
      List<Path> y = Files.list(x.get(0)).collect(Collectors.toList());
      sortFiles(y);
      return y.get(0);
    }

    /**
     * Fill in the paths to all tile files for the given resolution.
     *
     * @param descriptor the ResolutionDescriptor representing a resolution
     */
    private void findTileFiles(ResolutionDescriptor descriptor)
        throws IOException {
      if (!generateResolutions || descriptor.resolutionNumber == 0) {
        Path directory = inputDirectory.resolve(
          String.valueOf(descriptor.resolutionNumber));
        List<Path> x = Files.list(directory).collect(Collectors.toList());
        sortFiles(x);
        for (int xx=0; xx<x.size(); xx++) {
          Path xPath = x.get(xx);
          List<Path> y = Files.list(xPath).collect(Collectors.toList());
          sortFiles(y);

          for (int yy=0; yy<descriptor.tileFiles[xx].length; yy++) {
            int copy =
              (int) Math.min(planeCount, y.size() - yy * planeCount);
            if (copy > 0 && yy * planeCount < y.size()) {
              for (int p=0; p<copy; p++) {
                descriptor.tileFiles[xx][yy][p] = y.get(yy * planeCount + p);
              }
            }
          }
        }
      }
    }

    /**
     * Get the label image file, which may or may not exist.
     *
     * @return Path representing the expected label image file
     */
    private Path getLabelFile() {
      return inputDirectory.resolve(LABEL_FILE);
    }

    /**
     * Get the macro image file, which may or may not exist.
     *
     * @return Path representing the expected macro image file
     */
    private Path getMacroFile() {
      return inputDirectory.resolve(MACRO_FILE);
    }

    /**
     * Get the JSON metadata file, which may or may not exist.
     *
     * @return Path representing the expected JSON metadata file
     */
    private Path getMetadataFile() {
      return inputDirectory.resolve(METADATA_FILE);
    }

    /**
     * Get the OME-XML metadata file, which may or may not exist.
     *
     * @return Path representing the expected OME-XML metadata file
     */
    private Path getOMEXMLFile() {
      return inputDirectory.resolve(OMEXML_FILE);
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
        int no, int x, int y, Region region)
        throws FormatException, IOException
    {
        ResolutionDescriptor descriptor = resolutions.get(resolution);
        int bpp = FormatTools.getBytesPerPixel(pixelType);
        int xy = descriptor.tileSizeX * descriptor.tileSizeY;
        if (region != null) {
            xy = region.width * region.height;
        }

        if (n5Reader != null) {
            String blockPath = "/" + resolution;
            DataBlock block = n5Reader.readBlock(
                blockPath, n5Reader.getDatasetAttributes(blockPath),
                new long[] {x, y, no});
            ByteBuffer buffer = block.toByteBuffer();
            byte[] tile = new byte[xy * bpp * rgbChannels];
            if (region == null || (region.width == descriptor.tileSizeX &&
              region.height == descriptor.tileSizeY))
            {
                buffer.get(tile);
            }
            else {
                for (int ch=0; ch<rgbChannels; ch++) {
                    int tilePos = ch * xy * bpp;
                    int pos =
                        ch * descriptor.tileSizeX * descriptor.tileSizeY * bpp;
                    buffer.position(pos);
                    for (int row=0; row<region.height; row++) {
                        buffer.get(tile, tilePos, region.width * bpp);
                        buffer.position(buffer.position() +
                            (descriptor.tileSizeX - region.width) * bpp);
                        tilePos += region.width;
                    }
                }
            }
            return tile;
        }

        Path path = descriptor.tileFiles[x][y][no];
        if (path == null) {
            return null;
        }
        if (!Files.exists(path)) {
            return new byte[xy * bpp * rgbChannels];
        }
        try {
            StopWatch t0 = new Slf4JStopWatch("getInputTileBytes.setId");
            try {
                helperReader.setId(path.toString());
            }
            finally {
                t0.stop();
            }
            if (region == null) {
                t0 = new Slf4JStopWatch("getInputTileBytes.openBytes");
                try {
                    return helperReader.openBytes(0);
                }
                finally {
                    t0.stop();
                }
            }
            t0 = new Slf4JStopWatch("getInputTileBytes.openBytesWithRegion");
            try {
                return helperReader.openBytes(
                        0, 0, 0, region.width, region.height);
            }
            finally {
                t0.stop();
            }
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
            descriptor.resolutionNumber = resolution;

            if (n5Reader == null) {
                Path file = getFirstTileFile(resolution);
                helperReader.setId(file.toString());
                descriptor.tileSizeX = helperReader.getSizeX();
                descriptor.tileSizeY = helperReader.getSizeY();
                int[] tileCount = findNumberOfTiles(resolution);
                descriptor.numberOfTilesX = tileCount[0];
                descriptor.numberOfTilesY = tileCount[1];
            }
            else {
                DatasetAttributes attrs =
                    n5Reader.getDatasetAttributes("/" + resolution);
                descriptor.tileSizeX = attrs.getBlockSize()[0];
                descriptor.tileSizeY = attrs.getBlockSize()[1];
                rgbChannels = attrs.getBlockSize()[2];
                descriptor.numberOfTilesX =
                  (int) Math.ceil(
                      (double) attrs.getDimensions()[0] / descriptor.tileSizeX);
                descriptor.numberOfTilesY =
                  (int) Math.ceil(
                      (double) attrs.getDimensions()[1] / descriptor.tileSizeY);
            }

            if (resolution == 0) {
                if (metadata.getImageCount() > 0) {
                    descriptor.sizeX =
                      metadata.getPixelsSizeX(0).getNumberValue().intValue();
                    descriptor.sizeY =
                      metadata.getPixelsSizeY(0).getNumberValue().intValue();
                }
                else {
                    descriptor.sizeX =
                      descriptor.tileSizeX * descriptor.numberOfTilesX;
                    descriptor.sizeY =
                      descriptor.tileSizeY * descriptor.numberOfTilesY;
                }
            }
            else {
                descriptor.sizeX =
                    resolutions.get(resolution - 1).sizeX / PYRAMID_SCALE;
                descriptor.sizeY =
                    resolutions.get(resolution - 1).sizeY / PYRAMID_SCALE;
            }

            if (n5Reader == null) {
                descriptor.tileFiles = new Path[descriptor.numberOfTilesX][descriptor.numberOfTilesY][planeCount];
                findTileFiles(descriptor);
                log.info("Resolution: {}; Size: [{}, {}]; " +
                      "Grid size: [{}, {}]",
                      resolution, descriptor.sizeX, descriptor.sizeY,
                      descriptor.numberOfTilesX, descriptor.numberOfTilesY);
            }

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
    private void findNumberOfResolutions() throws IOException {
        if (n5Reader == null) {
            numberOfResolutions = Files.list(inputDirectory)
                .filter(Files::isDirectory)
                .collect(Collectors.toList()).size();
        }
        else {
            numberOfResolutions = n5Reader.list("/").length;
        }
    }

    /**
     * @param resolution the resolution index
     * @return total scale factor between the given resolution and the full
     *         resolution image (resolution 0)
     */
    private double getScale(int resolution) {
        return Math.pow(PYRAMID_SCALE, resolution);
    }

    /**
     * Return number of tiles in X, Y for a given resolution
     *
     * @param resolution pyramid resolution index, starting from 0
     * @return int array of length 2, containing the number of tiles in X and Y
     */
    private int[] findNumberOfTiles(int resolution) throws IOException {
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

        // only list directories, ignoring label/macro/metadata files
        List<Path> resolutionDirectories = Files.list(inputDirectory)
            .filter(Files::isDirectory)
            .collect(Collectors.toList());
        sortFiles(resolutionDirectories);
        Path resolutionDirectory = resolutionDirectories.get(resolution);
        List<Path> xDirectories = Files.list(resolutionDirectory)
            .collect(Collectors.toList());
        int tilesX = xDirectories.size();
        List<Path> yDirectories = Files.list(xDirectories.get(0))
            .collect(Collectors.toList());
        int tilesY = yDirectories.size() / planeCount;
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

        if (n5Reader == null) {
            helperReader.setOriginalMetadataPopulated(true);
            MetadataOptions options = new DefaultMetadataOptions();
            options.setValidate(true);
            helperReader.setMetadataOptions(options);
            // Read metadata from the first tile on the grid
            helperReader.setId(this.getFirstTileFile(0).toString());
            this.pixelType = helperReader.getPixelType();
            rgbChannels = helperReader.getRGBChannelCount();
            this.littleEndian = helperReader.isLittleEndian();
            interleaved = helperReader.isInterleaved();
        }
        else {
            interleaved = false;
            rgbChannels = 1;
            String blockPath = "/0";
            DataBlock block = n5Reader.readBlock(blockPath,
                n5Reader.getDatasetAttributes(blockPath), new long[] {0, 0, 0});
            littleEndian =
                block.toByteBuffer().order() == ByteOrder.LITTLE_ENDIAN;
            if (block instanceof ByteArrayDataBlock) {
                pixelType = FormatTools.UINT8;
            }
            else if (block instanceof ShortArrayDataBlock) {
                pixelType = FormatTools.UINT16;
            }
            else if (block instanceof IntArrayDataBlock) {
                pixelType = FormatTools.UINT32;
            }
            else if (block instanceof FloatArrayDataBlock) {
                pixelType = FormatTools.FLOAT;
            }
            else if (block instanceof DoubleArrayDataBlock) {
                pixelType = FormatTools.DOUBLE;
            }
            else {
                throw new FormatException("Unsupported block type: " + block);
            }
        }
    }

    /**
     * Set up the TIFF writer with all necessary metadata.
     * After this method is called, image data can be written.
     */
    public void initialize() throws FormatException, IOException {
        if (FormatHandler.checkSuffix(inputDirectory.toString(), "zarr")) {
          n5Reader = new N5ZarrReader(
              inputDirectory.resolve("pyramid.zarr").toString());
        }
        else if (FormatHandler.checkSuffix(inputDirectory.toString(), "n5")) {
          n5Reader = new N5FSReader(
              inputDirectory.resolve("pyramid.n5").toString());
        }

        log.info("Creating tiled pyramid file {}", this.outputFilePath);
        populateMetadataFromInputFile();

        OMEXMLService service = getService();
        Hashtable<String, Object> originalMeta =
            new Hashtable<String, Object>();
        if (service != null) {
            Path omexml = getOMEXMLFile();
            String xml = null;
            if (omexml != null && Files.exists(omexml)) {
                xml = DataTools.readFile(omexml.toString());
            }
            try {
                if (xml != null) {
                    metadata =
                        (OMEPyramidStore) service.createOMEXMLMetadata(xml);

                    z = metadata.getPixelsSizeZ(0).getNumberValue().intValue();
                    c = metadata.getPixelsSizeC(0).getNumberValue().intValue();
                    t = metadata.getPixelsSizeT(0).getNumberValue().intValue();
                    planeCount = (z * c * t) / rgbChannels;
                }
                else {
                    metadata = (OMEPyramidStore) service.createOMEXMLMetadata();
                }
            }
            catch (ServiceException e) {
                throw new FormatException("Could not parse OME-XML", e);
            }

            Path metadataFile = getMetadataFile();
            if (metadataFile != null && Files.exists(metadataFile)) {
                String jsonMetadata =
                    DataTools.readFile(metadataFile.toString());
                JSONObject json = new JSONObject(jsonMetadata);

                parseJSONValues(json, originalMeta, "");

                service.populateOriginalMetadata(metadata, originalMeta);
            }
        }

        describePyramid();

        for (ResolutionDescriptor descriptor : resolutions) {
            log.info("Adding metadata for resolution: {}",
                descriptor.resolutionNumber);

            String levelKey =
                "Image #0 | Level sizes #" + descriptor.resolutionNumber;
            Object realX = originalMeta.get(levelKey + " | X");
            Object realY = originalMeta.get(levelKey + " | Y");
            if (realX != null) {
                descriptor.sizeX = DataTools.parseDouble(realX.toString()).intValue();
            }
            if (realY != null) {
                descriptor.sizeY = DataTools.parseDouble(realY.toString()).intValue();
            }

            if (descriptor.resolutionNumber == 0) {
                MetadataTools.populateMetadata(
                    this.metadata, 0, null, this.littleEndian, "XYCZT",
                    FormatTools.getPixelTypeString(this.pixelType),
                    descriptor.sizeX, descriptor.sizeY, z,
                    rgbChannels * c, t, rgbChannels);
            }
            else {
                if (legacy) {
                    MetadataTools.populateMetadata(this.metadata,
                        descriptor.resolutionNumber, null, this.littleEndian,
                        "XYCZT", FormatTools.getPixelTypeString(pixelType),
                        descriptor.sizeX, descriptor.sizeY,
                        z, rgbChannels * c, t, rgbChannels);
                }
                else {
                    metadata.setResolutionSizeX(new PositiveInteger(
                        descriptor.sizeX), 0, descriptor.resolutionNumber);
                    metadata.setResolutionSizeY(new PositiveInteger(
                        descriptor.sizeY), 0, descriptor.resolutionNumber);
                }
            }
        }

        attachExtraImageMetadata();

        if (legacy) {
            writer = new TiffWriter();
        }
        else {
            writer = new PyramidOMETiffWriter();
        }
        writer.setBigTiff(true);
        writer.setWriteSequentially(true);
        writer.setMetadataRetrieve(this.metadata);
        if (compression != null) {
            writer.setCompression(compression);
        }
        writer.setInterleaved(interleaved);
        writer.setId(this.outputFilePath.toString());
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
            if (legacy) {
                writer.setSeries(resolution);
            }
            else {
                writer.setResolution(resolution);
            }
            for (int plane=0; plane<planeCount; plane++) {
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
                            StopWatch t0 = new Slf4JStopWatch("getInputTileBytes");
                            byte[] tileBytes;
                            try {
                                tileBytes =
                                    getInputTileBytes(resolution, plane, x, y, region);
                            }
                            finally {
                                t0.stop();
                            }
                            t0 = new Slf4JStopWatch("writeTile");
                            try {
                                if (tileBytes != null) {
                                    writeTile(plane, tileBytes, region, ifd);
                                }
                            }
                            finally {
                                t0.stop();
                            }
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
                            byte[] fullTile = getInputTileBytes(0, plane, x, y, null);
                            byte[] downsampled = scaler.downsample(fullTile,
                                zero.tileSizeX, zero.tileSizeY,
                                scale, bpp, littleEndian,
                                FormatTools.isFloatingPoint(pixelType),
                                rgbChannels, interleaved);
                            writeTile(plane, downsampled, region, ifd);
                        }
                    }
                }
            }
        }

        if (!legacy) {
            writer.setResolution(0);

            // add the label image, if present
            Path label = getLabelFile();
            int nextImage = 1;
            if (label != null && Files.exists(label)) {
                writer.setSeries(nextImage);
                try {
                    helperReader.setId(label.toString());
                    writer.saveBytes(0, helperReader.openBytes(0));
                }
                finally {
                    helperReader.close();
                }
                nextImage++;
            }

            // add the macro image, if present
            Path macro = getMacroFile();
            if (macro != null && Files.exists(macro)) {
                writer.setSeries(nextImage);
                try {
                    helperReader.setId(macro.toString());
                    writer.saveBytes(0, helperReader.openBytes(0));
                }
                finally {
                    helperReader.close();
                }
            }
        }

        this.writer.close();
        setPyramidIdentifier();
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
     * Wrapper for
     * {@link com.google.common.io.Files#getNameWithoutExtension(String)}
     */
    private String getNameWithoutExtension(Path path) {
      return com.google.common.io.Files.getNameWithoutExtension(
	      path.toString());
    }

    /**
     * Sort a list of files names numerically,
     * ignoring the .tiff extension if present.
     */
    private void sortFiles(List<Path> list) {
      // may be in the form:
      //
      // <int coordinate>
      //
      // or:
      //
      // <int coordinate>_w<int>_z<int>_t<int>
      if (list.size() > 0
          && list.get(0).getFileName().toString().split("_").length == 4) {
        Comparator<Path> c = Comparator.comparingInt(
          v -> Integer.parseInt(getNameWithoutExtension(v).split("_")[0]));
        c = c.thenComparingInt(
          v -> Integer.parseInt(getNameWithoutExtension(v).split("_")[1].substring(1)));
        c = c.thenComparingInt(
          v -> Integer.parseInt(getNameWithoutExtension(v).split("_")[2].substring(1)));
        c = c.thenComparingInt(
          v -> Integer.parseInt(getNameWithoutExtension(v).split("_")[3].substring(1)));
        list.sort(c);
      }
      else {
        list.sort(Comparator.comparingInt(
          v -> Integer.parseInt(getNameWithoutExtension(v))));
      }
    }

    private OMEXMLService getService() {
        try {
            ServiceFactory factory = new ServiceFactory();
            return factory.getInstance(OMEXMLService.class);
        }
        catch (DependencyException e) {
            log.warn("Could not create OME-XML service", e);
        }
        return null;
    }

    /**
     * Add metadata for label and macro images (if present) to
     * the current MetadataStore.  This does nothing if 5.9.x ("Faas")
     * pyramids are being written.
     */
    private void attachExtraImageMetadata() throws FormatException, IOException
    {
        if (legacy) {
            // Faas pyramid files can only contain the pyramid,
            // not any label/macro images
            return;
        }
        Path label = getLabelFile();
        Path macro = getMacroFile();

        int nextImage = 1;
        if (label != null && Files.exists(label)) {
            try {
                helperReader.setId(label.toString());
                MetadataTools.populateMetadata(metadata, nextImage,
                    "Label", helperReader.getCoreMetadataList().get(0));
                nextImage++;
            }
            finally {
                helperReader.close();
            }
        }
        if (macro != null && Files.exists(macro)) {
            try {
                helperReader.setId(macro.toString());
                MetadataTools.populateMetadata(metadata, nextImage,
                    "Macro", helperReader.getCoreMetadataList().get(0));
                nextImage++;
            }
            finally {
                helperReader.close();
            }
        }
    }

    /**
     * Update the first IFD so that the file will be detected as a pyramid.
     * This does nothing when writing 6.x-compatible OME-TIFF pyramids.
     */
    private void setPyramidIdentifier() throws FormatException, IOException {
        if (!legacy) {
            return;
        }

        try (RandomAccessInputStream in =
                new RandomAccessInputStream(outputFilePath.toString());
            RandomAccessOutputStream out =
                new RandomAccessOutputStream(outputFilePath.toString()))
        {
            TiffSaver saver = new TiffSaver(out, outputFilePath.toString());
            saver.overwriteIFDValue(in, 0, IFD.SOFTWARE, "Faas-raw2ometiff");
        }
    }

}
