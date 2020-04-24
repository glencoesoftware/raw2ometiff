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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.zarr.N5ZarrReader;

import ch.qos.logback.classic.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import loci.common.DataTools;
import loci.common.RandomAccessOutputStream;
import loci.common.Region;
import loci.common.services.DependencyException;
import loci.common.services.ServiceException;
import loci.common.services.ServiceFactory;
import loci.formats.ClassList;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.IFormatReader;
import loci.formats.ImageReader;
import loci.formats.MetadataTools;
import loci.formats.codec.CodecOptions;
import loci.formats.in.APNGReader;
import loci.formats.in.BMPReader;
import loci.formats.in.JPEGReader;
import loci.formats.in.MinimalTiffReader;
import loci.formats.ome.OMEPyramidStore;
import loci.formats.out.TiffWriter;
import loci.formats.services.OMEXMLService;
import loci.formats.tiff.IFD;
import loci.formats.tiff.IFDList;
import loci.formats.tiff.PhotoInterp;
import loci.formats.tiff.TiffCompression;
import loci.formats.tiff.TiffConstants;
import loci.formats.tiff.TiffSaver;
import ome.xml.meta.OMEXMLMetadataRoot;
import ome.xml.model.Channel;
import ome.xml.model.Pixels;
import ome.xml.model.primitives.NonNegativeInteger;
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
      }
      catch (Exception e) {
        return new ArrayList<String>();
      }
    }
  }

  private static final long FIRST_IFD_OFFSET = 8;

  /** Name of label image file. */
  private static final String LABEL_FILE = "LABELIMAGE.jpg";

  /** Name of macro image file. */
  private static final String MACRO_FILE = "MACROIMAGE.jpg";

  /** Name of JSON metadata file. */
  private static final String METADATA_FILE = "METADATA.json";

  /** Name of OME-XML metadata file. */
  private static final String OMEXML_FILE = "METADATA.ome.xml";

  private static final Logger LOG =
    LoggerFactory.getLogger(PyramidFromDirectoryWriter.class);

  /** Stream and writer for output TIFF file. */
  TiffSaver writer;
  RandomAccessOutputStream outStream;

  private BlockingQueue<Runnable> tileQueue;
  private ExecutorService executor;
  private IFDList[] ifds;

  /** Where to write? */
  @Parameters(
      index = "1",
      arity = "1",
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

  @Option(
      names = "--max_workers",
      description = "Maximum number of workers (default: ${DEFAULT-VALUE})"
  )
  int maxWorkers = Runtime.getRuntime().availableProcessors();

  @Option(
      names = "--rgb",
      description = "Attempt to write channels as RGB; channel count must be 3"
  )
  boolean rgb = false;

  /** FormatTools pixel type. */
  Integer pixelType;

  /** Writer metadata. */
  OMEPyramidStore metadata;

  /** Number of resolutions. */
  int numberOfResolutions;

  boolean littleEndian;

  int planeCount = 1;
  int z = 1;
  int c = 1;
  int t = 1;

  /** Description of each resolution in the pyramid. */
  List<ResolutionDescriptor> resolutions;

  private class ResolutionDescriptor {

    /** Resolution index (0 = the original image). */
    Integer resolutionNumber;

    /** Image width at this resolution. */
    Integer sizeX;

    /** Image height at this resolution. */
    Integer sizeY;

    /** Tile width at this resolution. */
    Integer tileSizeX;

    /** Tile height at this resolution. */
    Integer tileSizeY;

    /** Number of tiles along X axis. */
    Integer numberOfTilesX;

    /** Number of tiles along Y axis. */
    Integer numberOfTilesY;
  }

  /** Reader used for opening tile files. */
  private IFormatReader helperReader = null;

  private N5FSReader n5Reader = null;

  /**
   * Construct a writer for performing the pyramid conversion.
   */
  public PyramidFromDirectoryWriter() {
    // specify a minimal list of readers to speed up tile reading
    ClassList<IFormatReader> validReaders =
      new ClassList<IFormatReader>(IFormatReader.class);
    validReaders.addClass(BMPReader.class);
    validReaders.addClass(MinimalTiffReader.class);
    validReaders.addClass(JPEGReader.class);
    validReaders.addClass(APNGReader.class);
    helperReader = new ImageReader(validReaders);

    tileQueue = new LimitedQueue<Runnable>(maxWorkers);
    executor = new ThreadPoolExecutor(
      maxWorkers, maxWorkers, 0L, TimeUnit.MILLISECONDS, tileQueue);
  }

  /**
   * Convert a pyramid based upon the provided command line arguments.
   * @param args command line arguments
   */
  public static void main(String[] args) {
    CommandLine.call(new PyramidFromDirectoryWriter(), args);
  }

  @Override
  public Void call() throws Exception {
    ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)
      LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    if (debug) {
      root.setLevel(Level.DEBUG);
    }
    else {
      root.setLevel(Level.INFO);
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
   * Provide tile from the input folder.
   *
   * @param resolution the pyramid level, indexed from 0 (largest)
   * @param no the plane index
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
    int tileSize = xy * bpp;
    byte[] tile = new byte[tileSize];
    String blockPath = "/" + resolution;
    long[] gridPosition = new long[] {x, y, no};
    DataBlock<?> block = n5Reader.readBlock(
      blockPath, n5Reader.getDatasetAttributes(blockPath),
      gridPosition);

    if (block == null) {
      throw new FormatException("Could not find block " + blockPath);
    }

    ByteBuffer buffer = block.toByteBuffer();
    boolean isPadded = buffer.limit() > tileSize;
    if (region == null || (region.width == descriptor.tileSizeX &&
      region.height == descriptor.tileSizeY))
    {
      buffer.get(tile);
    }
    else {
      int tilePos = 0;
      for (int row=0; row<region.height; row++) {
        buffer.get(tile, tilePos, region.width * bpp);
        if (isPadded) {
          buffer.position(buffer.position() +
            (descriptor.tileSizeX - region.width) * bpp);
        }
        tilePos += region.width * bpp;
      }
    }
    return tile;
  }

  /**
   * Calculate image width and height for each resolution.
   * Uses the first tile in the resolution to find the tile size.
   */
  public void describePyramid() throws FormatException, IOException {
    LOG.info("Number of resolution levels: {}", numberOfResolutions);
    resolutions = new ArrayList<ResolutionDescriptor>();
    for (int resolution = 0; resolution < numberOfResolutions; resolution++) {
      ResolutionDescriptor descriptor = new ResolutionDescriptor();
      descriptor.resolutionNumber = resolution;

      DatasetAttributes attrs = n5Reader.getDatasetAttributes("/" + resolution);
      descriptor.sizeX = (int) attrs.getDimensions()[0];
      descriptor.sizeY = (int) attrs.getDimensions()[1];
      descriptor.tileSizeX = attrs.getBlockSize()[0];
      descriptor.tileSizeY = attrs.getBlockSize()[1];
      descriptor.numberOfTilesX =
        getTileCount(descriptor.sizeX, descriptor.tileSizeX);
      descriptor.numberOfTilesY =
        getTileCount(descriptor.sizeY, descriptor.tileSizeY);

      if (resolution == 0) {
        // If we have image metadata available sanity the dimensions against
        // those in the underlying N5 pyramid
        if (metadata.getImageCount() > 0) {
          int sizeX = metadata.getPixelsSizeX(0).getNumberValue().intValue();
          int sizeY = metadata.getPixelsSizeY(0).getNumberValue().intValue();
          if (descriptor.sizeX != sizeX) {
            throw new FormatException(String.format(
                "Resolution %d dimension mismatch! metadata=%d pyramid=%d",
                resolution, descriptor.sizeX, sizeX));
          }
          if (descriptor.sizeY != sizeY) {
            throw new FormatException(String.format(
                "Resolution %d dimension mismatch! metadata=%d pyramid=%d",
                resolution, descriptor.sizeY, sizeY));
          }
        }
      }

      resolutions.add(descriptor);
    }
  }

  /**
   * Calculate the number of resolutions based upon the number
   * of directories in the base input directory.
   */
  private void findNumberOfResolutions() throws IOException {
    numberOfResolutions = n5Reader.list("/").length;
  }

  /**
   * Set up the TIFF writer with all necessary metadata.
   * After this method is called, image data can be written.
   */
  public void initialize()
    throws FormatException, IOException, DependencyException
  {
    Path zarr = inputDirectory.resolve("pyramid.zarr");
    if (Files.exists(zarr)) {
      n5Reader = new N5ZarrReader(zarr.toString());
    }
    Path n5 = inputDirectory.resolve("pyramid.n5");
    if (Files.exists(n5)) {
      n5Reader = new N5FSReader(n5.toString());
    }

    if (n5Reader == null) {
      throw new FormatException("Could not create an N5 reader");
    }

    LOG.info("Creating tiled pyramid file {}", this.outputFilePath);
    findNumberOfResolutions();

    OMEXMLService service = getService();
    Hashtable<String, Object> originalMeta = new Hashtable<String, Object>();
    if (service != null) {
      Path omexml = getOMEXMLFile();
      String xml = null;
      if (omexml != null && Files.exists(omexml)) {
        xml = DataTools.readFile(omexml.toString());
      }
      try {
        if (xml != null) {
          metadata = (OMEPyramidStore) service.createOMEXMLMetadata(xml);

          z = metadata.getPixelsSizeZ(0).getNumberValue().intValue();
          c = metadata.getPixelsSizeC(0).getNumberValue().intValue();
          t = metadata.getPixelsSizeT(0).getNumberValue().intValue();
          planeCount = z * t;
          littleEndian = !metadata.getPixelsBigEndian(0);
          pixelType = FormatTools.pixelTypeFromString(
            metadata.getPixelsType(0).getValue());

          rgb = rgb && (c == 3) && (z * t == 1);
          if (!rgb) {
            planeCount *= c;
          }

          OMEXMLMetadataRoot root = (OMEXMLMetadataRoot) metadata.getRoot();
          for (int image = 0; image<root.sizeOfImageList(); image++) {
            Pixels pixels = root.getImage(image).getPixels();
            pixels.setMetadataOnly(null);
            if (rgb) {
              while (pixels.sizeOfChannelList() > 1) {
                Channel ch = pixels.getChannel(pixels.sizeOfChannelList() - 1);
                pixels.removeChannel(ch);
              }
              Channel onlyChannel = pixels.getChannel(0);
              onlyChannel.setSamplesPerPixel(new PositiveInteger(c));
            }
          }
        }
        else {
          metadata = (OMEPyramidStore) service.createOMEXMLMetadata();
        }
      }
      catch (ServiceException e) {
        throw new FormatException("Could not parse OME-XML", e);
      }
    }

    Path metadataFile = getMetadataFile();
    if (metadataFile != null && Files.exists(metadataFile)) {
      String jsonMetadata = DataTools.readFile(metadataFile.toString());
      JSONObject json = new JSONObject(jsonMetadata);

      parseJSONValues(json, originalMeta, "");

      service.populateOriginalMetadata(metadata, originalMeta);
    }

    describePyramid();

    for (ResolutionDescriptor descriptor : resolutions) {
      LOG.info("Adding metadata for resolution: {}",
        descriptor.resolutionNumber);

      if (descriptor.resolutionNumber == 0) {
        MetadataTools.populateMetadata(
          this.metadata, 0, null, this.littleEndian, "XYCZT",
          FormatTools.getPixelTypeString(this.pixelType),
          descriptor.sizeX, descriptor.sizeY, z, c, t, rgb ? c : 1);
      }
      else {
        if (legacy) {
          MetadataTools.populateMetadata(this.metadata,
            descriptor.resolutionNumber, null, this.littleEndian,
            "XYCZT", FormatTools.getPixelTypeString(pixelType),
            descriptor.sizeX, descriptor.sizeY, z, c, t, rgb ? c : 1);
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

    outStream = new RandomAccessOutputStream(outputFilePath.toString());
    writer = new TiffSaver(outStream, outputFilePath.toString());
    writer.setBigTiff(true);
    writer.setLittleEndian(littleEndian);
    writer.writeHeader();
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
   * Writes all image data to the initialized TIFF writer.
   */
  public void convertToPyramid()
    throws FormatException, IOException,
      InterruptedException, DependencyException
  {
    // convert every resolution in the pyramid
    ifds = new IFDList[numberOfResolutions];
    for (int resolution=0; resolution<numberOfResolutions; resolution++) {
      ifds[resolution] = new IFDList();
      for (int plane=0; plane<planeCount; plane++) {
        IFD ifd = makeIFD(resolution, plane);
        ifds[resolution].add(ifd);
      }
    }

    int rgbChannels = rgb ? c : 1;

    try {
      for (int resolution=0; resolution<numberOfResolutions; resolution++) {
        LOG.info("Converting resolution #{}", resolution);
        ResolutionDescriptor descriptor = resolutions.get(resolution);
        int tileCount = descriptor.numberOfTilesY * descriptor.numberOfTilesX;
        for (int plane=0; plane<planeCount; plane++) {
          int tileIndex = 0;
          // if the resolution has already been calculated,
          // just read each tile from disk and store in the OME-TIFF
          for (int y = 0; y < descriptor.numberOfTilesY; y++) {
            for (int x = 0; x < descriptor.numberOfTilesX; x++, tileIndex++) {
              Region region = new Region(
                x * descriptor.tileSizeX,
                y * descriptor.tileSizeY, 0, 0);
              region.width = (int) Math.min(
                descriptor.tileSizeX, descriptor.sizeX - region.x);
              region.height = (int) Math.min(
                descriptor.tileSizeY, descriptor.sizeY - region.y);

              if (region.width <= 0 || region.height <= 0) {
                continue;
              }

              for (int ch=0; ch<rgbChannels; ch++) {
                StopWatch t0 = new Slf4JStopWatch("getInputTileBytes");
                byte[] tileBytes;
                try {
                  int planeIndex = plane * rgbChannels + ch;
                  tileBytes =
                    getInputTileBytes(resolution, planeIndex, x, y, region);
                }
                finally {
                  t0.stop();
                }

                final int currentIndex = tileCount * ch + tileIndex;
                final int currentPlane = plane;
                final int currentResolution = resolution;
                executor.execute(() -> {
                  Slf4JStopWatch t1 = new Slf4JStopWatch("writeTile");
                  try {
                    if (tileBytes != null) {
                      if (region.width == descriptor.tileSizeX) {
                        writeTile(currentPlane, tileBytes,
                          currentIndex, currentResolution);
                      }
                      else {
                        // pad the tile to the correct width
                        int paddedHeight = tileBytes.length / region.width;
                        byte[] realTile =
                          new byte[descriptor.tileSizeX * paddedHeight];
                        int totalRows = region.height;
                        int inRowLen = tileBytes.length / totalRows;
                        int outRowLen = realTile.length / totalRows;
                        for (int row=0; row<totalRows; row++) {
                          System.arraycopy(tileBytes, row * inRowLen,
                            realTile, row * outRowLen, inRowLen);
                        }
                        writeTile(currentPlane, realTile,
                          currentIndex, currentResolution);
                      }
                    }
                  }
                  catch (FormatException|IOException e) {
                    LOG.error("Failed to write tile in resolution {}",
                      currentResolution, e);
                  }
                  finally {
                    t1.stop();
                  }
                });
              }
            }
          }
        }
      }
    }
    finally {
      executor.shutdown();
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    IFD labelIFD = null;
    IFD macroIFD = null;

    if (!legacy) {
      // add the label image, if present
      Path label = getLabelFile();
      if (label != null && Files.exists(label)) {
        try {
          helperReader.setId(label.toString());
          labelIFD = makeIFD(helperReader);

          byte[] bytes = helperReader.openBytes(0);
          int channelBytes = bytes.length / helperReader.getRGBChannelCount();
          long[] offsets = labelIFD.getStripOffsets();
          long[] counts = labelIFD.getStripByteCounts();
          for (int s=0; s<offsets.length; s++) {
            offsets[s] = outStream.getFilePointer() + s * channelBytes;
            counts[s] = channelBytes;
          }

          labelIFD.put(IFD.STRIP_OFFSETS, offsets);
          labelIFD.put(IFD.STRIP_BYTE_COUNTS, counts);
          outStream.write(bytes);
        }
        finally {
          helperReader.close();
        }
      }

      // add the macro image, if present
      Path macro = getMacroFile();
      if (macro != null && Files.exists(macro)) {
        try {
          helperReader.setId(macro.toString());
          macroIFD = makeIFD(helperReader);

          byte[] bytes = helperReader.openBytes(0);
          int channelBytes = bytes.length / helperReader.getRGBChannelCount();
          long[] offsets = macroIFD.getStripOffsets();
          long[] counts = macroIFD.getStripByteCounts();
          for (int s=0; s<offsets.length; s++) {
            offsets[s] = outStream.getFilePointer() + s * channelBytes;
            counts[s] = channelBytes;
          }

          macroIFD.put(IFD.STRIP_OFFSETS, offsets);
          macroIFD.put(IFD.STRIP_BYTE_COUNTS, counts);
          outStream.write(bytes);
        }
        finally {
          helperReader.close();
        }
      }
    }

    long firstIFD = outStream.getFilePointer();

    if (legacy) {
      for (int resolution=0; resolution<numberOfResolutions; resolution++) {
        for (int plane=0; plane<planeCount; plane++) {
          boolean last = (resolution == numberOfResolutions - 1) &&
            (plane == planeCount -1);
          writeIFD(resolution, plane, !last);
        }
      }
    }
    else {
      long[][] subs = new long[planeCount][numberOfResolutions - 1];
      for (int plane=0; plane<planeCount; plane++) {
        for (int resolution=1; resolution<numberOfResolutions; resolution++) {
          subs[plane][resolution - 1] = outStream.getFilePointer();
          writeIFD(resolution, plane, resolution < numberOfResolutions - 1);
        }
      }
      firstIFD = outStream.getFilePointer();
      for (int plane=0; plane<planeCount; plane++) {
        ifds[0].get(plane).put(IFD.SUB_IFD, subs[plane]);
        writeIFD(0, plane,
          plane < planeCount - 1 || labelIFD != null || macroIFD != null);
      }
      if (labelIFD != null) {
        int ifdSize = getIFDSize(labelIFD);
        long offsetPointer = outStream.getFilePointer() + ifdSize;
        writer.writeIFD(labelIFD, 0);
        if (macroIFD != null) {
          overwriteNextOffset(offsetPointer);
        }
      }
      if (macroIFD != null) {
        writer.writeIFD(macroIFD, 0);
      }
    }
    outStream.seek(FIRST_IFD_OFFSET);
    outStream.writeLong(firstIFD);

    this.writer.close();
    outStream.close();
  }

  /**
   * Overwrite an IFD offset at the given pointer.
   * The output stream should be positioned to the new IFD offset
   * before this method is called.
   *
   * @param offsetPointer pointer to the IFD offset that will be overwritten
   */
  private void overwriteNextOffset(long offsetPointer) throws IOException {
    long fp = outStream.getFilePointer();
    outStream.seek(offsetPointer);
    outStream.writeLong(fp);
    outStream.seek(fp);
  }

  /**
   * Get the size in bytes of the given IFD.
   * This size includes the IFD header and tags that would be written,
   * but does not include the size of any non-inlined tag values.
   * This is mainly used to calculate the file pointer at which to write
   * the offset to the next IFD.
   *
   * @param ifd the IFD for which to calculate a size
   * @return the number of bytes in the IFD
   */
  private int getIFDSize(IFD ifd) {
    // subtract LITTLE_ENDIAN from the key count
    return 8 + TiffConstants.BIG_TIFF_BYTES_PER_ENTRY * (ifd.size() - 1);
  }

  /**
   * Create an IFD using the given reader's current state.
   * All relevant tags are filled in; strip data is filled with placeholders.
   * This should only be used for extra images as it assumes that pixel
   * data is stored in a single strip instead of tiles.
   *
   * @param reader the initialized image reader that supplies metadata
   * @return an IFD that is ready to be filled with strip data
   */
  private IFD makeIFD(IFormatReader reader) {
    IFD ifd = new IFD();
    ifd.put(IFD.LITTLE_ENDIAN, reader.isLittleEndian());
    ifd.put(IFD.IMAGE_WIDTH, (long) reader.getSizeX());
    ifd.put(IFD.IMAGE_LENGTH, (long) reader.getSizeY());
    ifd.put(IFD.ROWS_PER_STRIP, reader.getSizeY());
    ifd.put(IFD.PLANAR_CONFIGURATION,
      reader.isInterleaved() || reader.getRGBChannelCount() == 1 ? 1 : 2);
    ifd.put(IFD.SAMPLE_FORMAT, 1);

    int[] bps = new int[1];
    Arrays.fill(bps, FormatTools.getBytesPerPixel(reader.getPixelType()) * 8);
    ifd.put(IFD.BITS_PER_SAMPLE, bps);

    ifd.put(IFD.SAMPLES_PER_PIXEL, reader.getRGBChannelCount());
    ifd.put(IFD.PHOTOMETRIC_INTERPRETATION,
      reader.getRGBChannelCount() == 1 ?
      PhotoInterp.BLACK_IS_ZERO.getCode() : PhotoInterp.RGB.getCode());

    ifd.put(IFD.SOFTWARE, FormatTools.CREATOR);
    ifd.put(IFD.STRIP_BYTE_COUNTS, new long[reader.getRGBChannelCount()]);
    ifd.put(IFD.STRIP_OFFSETS, new long[reader.getRGBChannelCount()]);
    ifd.put(IFD.COMPRESSION, TiffCompression.UNCOMPRESSED.getCode());
    return ifd;
  }

  /**
   * Create an IFD for the given plane in the given resolution.
   * All relevant tags are filled in; sub-IFD and tile data are
   * filled with placeholders.
   *
   * @param resolution the resolution index for the new IFD
   * @param plane the plane index for the new IFD
   * @return an IFD that is ready to be filled with tile data
   */
  private IFD makeIFD(int resolution, int plane)
    throws FormatException, DependencyException
  {
    IFD ifd = new IFD();
    ifd.put(IFD.LITTLE_ENDIAN, littleEndian);
    ResolutionDescriptor descriptor = resolutions.get(resolution);
    ifd.put(IFD.IMAGE_WIDTH, (long) descriptor.sizeX);
    ifd.put(IFD.IMAGE_LENGTH, (long) descriptor.sizeY);
    ifd.put(IFD.TILE_WIDTH, descriptor.tileSizeX);
    ifd.put(IFD.TILE_LENGTH, descriptor.tileSizeY);
    ifd.put(IFD.COMPRESSION, getTIFFCompression().getCode());

    ifd.put(IFD.PLANAR_CONFIGURATION, rgb ? 2 : 1);
    ifd.put(IFD.SAMPLE_FORMAT, 1);

    int[] bps = new int[rgb ? c : 1];
    Arrays.fill(bps, FormatTools.getBytesPerPixel(pixelType) * 8);
    ifd.put(IFD.BITS_PER_SAMPLE, bps);

    ifd.put(IFD.PHOTOMETRIC_INTERPRETATION,
      rgb ? PhotoInterp.RGB.getCode() : PhotoInterp.BLACK_IS_ZERO.getCode());
    ifd.put(IFD.SAMPLES_PER_PIXEL, bps.length);

    if (legacy) {
      ifd.put(IFD.SOFTWARE, "Faas-raw2ometiff");
    }
    else {
      ifd.put(IFD.SOFTWARE, FormatTools.CREATOR);
    }

    if (resolution == 0 && plane == 0) {
      try {
        metadata.setTiffDataIFD(new NonNegativeInteger(0), 0, 0);
        metadata.setTiffDataPlaneCount(
            new NonNegativeInteger(planeCount), 0, 0);

        for (int extra=1; extra<metadata.getImageCount(); extra++) {
          int ifdIndex = planeCount + extra - 1;
          metadata.setTiffDataIFD(new NonNegativeInteger(ifdIndex), extra, 0);
          metadata.setTiffDataPlaneCount(new NonNegativeInteger(1), 1, 0);
        }

        OMEXMLService service = getService();
        String omexml = service.getOMEXML(metadata);
        ifd.put(IFD.IMAGE_DESCRIPTION, omexml);
      }
      catch (ServiceException e) {
        throw new FormatException("Could not get OME-XML", e);
      }
    }

    int tileCount =
      descriptor.numberOfTilesX * descriptor.numberOfTilesY * bps.length;
    ifd.put(IFD.TILE_BYTE_COUNTS, new long[tileCount]);
    ifd.put(IFD.TILE_OFFSETS, new long[tileCount]);

    if (!legacy) {
      if (resolution == 0) {
        ifd.put(IFD.SUB_IFD, (long) 0);
      }
      else {
        ifd.put(IFD.NEW_SUBFILE_TYPE, 1);
      }
    }

    return ifd;
  }

  /**
   * Write a single tile to the writer's current resolution.
   *
   * @param imageNumber the plane number in the resolution
   * @param buffer the array containing the tile's pixel data
   * @param tileIndex index of the tile to be written in XY space
   * @param resolution resolution index of the tile
   */
  private void writeTile(
      Integer imageNumber, byte[] buffer, int tileIndex, int resolution)
      throws FormatException, IOException
  {
    LOG.debug("Writing image: {}, tileIndex: {}", imageNumber, tileIndex);

    IFD ifd = ifds[resolution].get(imageNumber);
    TiffCompression tiffCompression = getTIFFCompression();
    CodecOptions options = tiffCompression.getCompressionCodecOptions(ifd);
    options.width = (int) ifd.getTileWidth();
    options.height = (int) ifd.getTileLength();
    options.channels = 1;

    byte[] realTile = tiffCompression.compress(buffer, options);
    LOG.debug("    writing {} compressed bytes at {}",
      realTile.length, outStream.getFilePointer());

    writeToDisk(realTile, tileIndex, resolution, imageNumber);
  }

  /**
   * Write a pre-compressed buffer corresponding to the
   * given IFD and tile index.
   *
   * @param realTile array of compressed bytes representing the tile
   * @param tileIndex index into the array of tile offsets
   * @param resolution resolution index of the tile
   * @param imageNumber image index of the tile
   */
  private synchronized void writeToDisk(byte[] realTile, int tileIndex,
      int resolution, int imageNumber)
      throws FormatException, IOException
  {
    IFD ifd = ifds[resolution].get(imageNumber);

    // do not use ifd.getStripByteCounts() or ifd.getStripOffsets() here
    // as both can return values other than what is in the IFD
    long[] offsets = ifd.getIFDLongArray(IFD.TILE_OFFSETS);
    long[] byteCounts = ifd.getIFDLongArray(IFD.TILE_BYTE_COUNTS);
    offsets[tileIndex] = outStream.getFilePointer();
    byteCounts[tileIndex] = (long) realTile.length;

    outStream.write(realTile);

    ifd.put(IFD.TILE_OFFSETS, offsets);
    ifd.put(IFD.TILE_BYTE_COUNTS, byteCounts);
  }

  /**
   * Create an OMEXMLService for manipulating OME-XML metadata.
   *
   * @return OMEXMLService instance, or null if the service is not available
   */
  private OMEXMLService getService() throws DependencyException {
    ServiceFactory factory = new ServiceFactory();
    return factory.getInstance(OMEXMLService.class);
  }

  /**
   * Add metadata for label and macro images (if present) to
   * the current MetadataStore.  This does nothing if 5.9.x ("Faas")
   * pyramids are being written.
   */
  private void attachExtraImageMetadata() throws FormatException, IOException {
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
   * Convert the compression argument to a TiffCompression object that can
   * be used to compress tiles.
   *
   * @return TiffCompression corresponding to the compression argument,
   */
  private TiffCompression getTIFFCompression() {
    if (compression.equals(TiffWriter.COMPRESSION_LZW)) {
      return TiffCompression.LZW;
    }
    else if (compression.equals(TiffWriter.COMPRESSION_J2K)) {
      return TiffCompression.JPEG_2000;
    }
    else if (compression.equals(TiffWriter.COMPRESSION_J2K_LOSSY)) {
      return TiffCompression.JPEG_2000_LOSSY;
    }
    else if (compression.equals(TiffWriter.COMPRESSION_JPEG)) {
      return TiffCompression.JPEG;
    }
    else if (compression.equals(TiffWriter.COMPRESSION_ZLIB)) {
      return TiffCompression.DEFLATE;
    }
    return TiffCompression.UNCOMPRESSED;
  }

  /**
   * Write the IFD for the given resolution and plane.
   *
   * @param resolution the resolution index
   * @param plane the plane index
   * @param overwrite true unless this is the last IFD in the list
   */
  private void writeIFD(int resolution, int plane, boolean overwrite)
    throws FormatException, IOException
  {
    int ifdSize = getIFDSize(ifds[resolution].get(plane));
    long offsetPointer = outStream.getFilePointer() + ifdSize;
    writer.writeIFD(ifds[resolution].get(plane), 0);
    if (overwrite) {
      overwriteNextOffset(offsetPointer);
    }
  }

  /**
   * Calculate the number of tiles for a dimension based upon the tile size.
   *
   * @param size the number of pixels in the dimension (e.g. image width)
   * @param tileSize the number of pixels in the tile along the same dimension
   * @return the number of tiles
   */
  private int getTileCount(long size, long tileSize) {
    return (int) Math.ceil((double) size / tileSize);
  }

}
