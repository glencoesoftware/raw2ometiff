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
import java.util.Arrays;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import loci.common.Region;
import loci.formats.ClassList;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.IFormatReader;
import loci.formats.ImageReader;
import loci.formats.MetadataTools;
import loci.formats.in.DefaultMetadataOptions;
import loci.formats.in.JPEGReader;
import loci.formats.in.MetadataOptions;
import loci.formats.in.MinimalTiffReader;
import loci.formats.ome.OMEPyramidStore;
import loci.formats.out.PyramidOMETiffWriter;

import ome.xml.model.primitives.PositiveInteger;

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
public class PyramidFromDirectoryWriter {

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
    String outputFilePath;

    /** Where to write? */
    String inputDirectory;

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

    /** Store resolution info */
    ResolutionDescriptor[] resolutions;

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

    public PyramidFromDirectoryWriter(
        String inputDirectory, String outputFilePath)
    {
        this.inputDirectory = inputDirectory;
        this.outputFilePath = outputFilePath;

        // specify a minimal list of readers to speed up tile reading
        ClassList<IFormatReader> validReaders =
            new ClassList<IFormatReader>(IFormatReader.class);
        validReaders.addClass(MinimalTiffReader.class);
        validReaders.addClass(JPEGReader.class);
        helperReader = new ImageReader(validReaders);
    }

    public static void main(String[] args) throws FormatException, IOException {
        PyramidFromDirectoryWriter writer =
            new PyramidFromDirectoryWriter(args[0], args[1]);
        writer.initialize();
        writer.convertToPyramid();
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
     * @return byte array containing the pixels for the tile
     */
    private byte[] getInputTileBytes(int resolution, int x, int y)
        throws FormatException, IOException
    {
        ResolutionDescriptor descriptor = resolutions[resolution];
        String path = descriptor.tileFiles[x][y];
        if (path == null) {
            return null;
        }
        if (!new File(path).exists()) {
          return new byte[descriptor.tileSizeX * descriptor.tileSizeY *
            FormatTools.getBytesPerPixel(pixelType) * rgbChannels];
        }
        try {
            helperReader.setId(path);
            return helperReader.openBytes(0);
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
        resolutions = new ResolutionDescriptor[numberOfResolutions];
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
            descriptor.sizeX =
              descriptor.tileSizeX * descriptor.numberOfTilesX;
            descriptor.sizeY =
              descriptor.tileSizeY * descriptor.numberOfTilesY;
            descriptor.tileFiles =
              new String[descriptor.numberOfTilesX][descriptor.numberOfTilesY];
            findTileFiles(descriptor);
            log.info("Resolution: {}; Size: [{}, {}]; " +
                      "Grid size: [{}, {}]",
                      resolution, descriptor.sizeX, descriptor.sizeY,
                      descriptor.numberOfTilesX, descriptor.numberOfTilesY);
            resolutions[resolution] = descriptor;
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

    /**
     * Return number of tiles in X, Y for a given resolution
     *
     * @param resolution pyramid resolution index, starting from 0
     * @return int array of length 2, containing the number of tiles in X and Y
     */
    private int[] findNumberOfTiles(int resolution) {
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
        for (ResolutionDescriptor descriptor : resolutions) {
            log.info("Adding metadata for resolution: {}",
                descriptor.resolutionNumber);
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

        if (label != null && label.exists()) {
          // TODO: add to metadata
        }
        if (macro != null && macro.exists()) {
          // TODO: add to metadata
        }

        // TODO: add metadata file as annotation(s)

        writer = new PyramidOMETiffWriter();
        writer.setBigTiff(true);
        writer.setMetadataRetrieve(this.metadata);
        writer.setTileSizeX(resolutions[numberOfResolutions - 1].tileSizeX);
        writer.setTileSizeY(resolutions[numberOfResolutions - 1].tileSizeY);
        writer.setId(this.outputFilePath);
    }

     //* Conversion */

    /**
     * Writes all image data to the initialized TIFF writer
     * and amends the file type metadata to ensure that the
     * TIFF will be correctly detected as a pyramid.
     */
    public void convertToPyramid() throws FormatException, IOException {
        for (int resolution=0; resolution<numberOfResolutions; resolution++) {
            log.info("Converting resolution #{}", resolution);
            writer.setResolution(resolution);
            ResolutionDescriptor descriptor = resolutions[resolution];
            Region region =
                new Region(0, 0, descriptor.tileSizeX, descriptor.tileSizeY);
            for (int y = 0; y < descriptor.numberOfTilesY; y ++) {
                region.y = y * descriptor.tileSizeY;
                for (int x = 0; x < descriptor.numberOfTilesX; x++) {
                    region.x = x * descriptor.tileSizeX;
                    byte[] tileBytes = getInputTileBytes(resolution, x, y);
                    writeTile(0, tileBytes, region);
                }
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
            Integer imageNumber, byte[] buffer, Region region)
                    throws FormatException, IOException
    {
        log.debug("Writing image: {}, region: {}", imageNumber, region);
        this.writer.saveBytes(imageNumber, buffer, region);
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
