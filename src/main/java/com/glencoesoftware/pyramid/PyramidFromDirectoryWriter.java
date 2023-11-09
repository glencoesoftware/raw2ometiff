/**
 * Copyright (c) 2019-2020 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.pyramid;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import ch.qos.logback.classic.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import loci.common.Constants;
import loci.common.DataTools;
import loci.common.RandomAccessOutputStream;
import loci.common.Region;
import loci.common.services.DependencyException;
import loci.common.services.ServiceException;
import loci.common.services.ServiceFactory;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.MetadataTools;
import loci.formats.codec.CodecOptions;
import loci.formats.ome.OMEPyramidStore;
import loci.formats.services.OMEXMLService;
import loci.formats.tiff.IFD;
import loci.formats.tiff.IFDList;
import loci.formats.tiff.PhotoInterp;
import loci.formats.tiff.TiffCompression;
import loci.formats.tiff.TiffConstants;
import loci.formats.tiff.TiffRational;
import loci.formats.tiff.TiffSaver;
import ome.units.UNITS;
import ome.units.quantity.Length;
import ome.xml.meta.OMEXMLMetadataRoot;
import ome.xml.model.Channel;
import ome.xml.model.FilterSet;
import ome.xml.model.Pixels;
import ome.xml.model.enums.DimensionOrder;
import ome.xml.model.enums.EnumerationException;
import ome.xml.model.enums.PixelType;
import ome.xml.model.primitives.NonNegativeInteger;
import ome.xml.model.primitives.PositiveInteger;

import org.json.JSONObject;
import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;

import com.bc.zarr.DataType;
import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import com.glencoesoftware.bioformats2raw.IProgressListener;
import com.glencoesoftware.bioformats2raw.NoOpProgressListener;
import com.glencoesoftware.bioformats2raw.ProgressBarListener;
import ucar.ma2.InvalidRangeException;

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

  private static final long FIRST_IFD_OFFSET = 8;

  /** Name of JSON metadata file. */
  private static final String METADATA_FILE = "METADATA.json";

  /** Name of OME-XML metadata file. */
  private static final String OMEXML_FILE = "METADATA.ome.xml";

  private static final Logger LOG =
    LoggerFactory.getLogger(PyramidFromDirectoryWriter.class);

  /** Path to each output file. */
  private List<Path> seriesPaths;

  private BlockingQueue<Runnable> tileQueue;
  private ExecutorService executor;

  private IProgressListener progressListener;

  /** Where to write? */
  Path outputFilePath;
  Path inputDirectory;
  private volatile String logLevel = "WARN";
  private volatile boolean progressBars = false;
  boolean printVersion = false;
  CompressionType compression = CompressionType.LZW;
  CodecOptions compressionOptions;
  boolean legacy = false;
  int maxWorkers = Runtime.getRuntime().availableProcessors();
  boolean rgb = false;

  private List<PyramidSeries> series = new ArrayList<PyramidSeries>();
  private Map<String, TiffSaver> tiffSavers = new HashMap<String, TiffSaver>();
  boolean splitBySeries = false;

  private ZarrGroup reader = null;

  /** Writer metadata. */
  OMEPyramidStore metadata;
  OMEPyramidStore binaryOnly;

  private Map<String, Object> plateData = null;

  /**
   * Construct a writer for performing the pyramid conversion.
   */
  public PyramidFromDirectoryWriter() {
    tileQueue = new LimitedQueue<Runnable>(maxWorkers);
  }

  /**
   * Where to write?
   *
   * @param output path to output TIFF
   */
  @Parameters(
      index = "1",
      arity = "1",
      description = "Relative path to the output OME-TIFF file"
  )
  public void setOutputPath(String output) {
    // could be expanded to allow other output locations
    outputFilePath = Paths.get(output);
  }

  /**
   * Where to read?
   *
   * @param input path to input Zarr directory
   */
  @Parameters(
      index = "0",
      arity = "1",
      description = "Directory containing pixel data to convert"
  )
  public void setInputPath(String input) {
    // could be expanded to allow other input locations
    inputDirectory = Paths.get(input);
  }

  /**
   * Set the slf4j logging level. Defaults to "WARN".
   *
   * @param level logging level
   */
  @Option(
    names = {"--log-level", "--debug"},
    arity = "0..1",
    description = "Change logging level; valid values are " +
      "OFF, ERROR, WARN, INFO, DEBUG, TRACE and ALL. " +
      "(default: ${DEFAULT-VALUE})",
    defaultValue = "WARN",
    fallbackValue = "DEBUG"
  )
  public void setLogLevel(String level) {
    logLevel = level;
  }

  /**
   * Configure whether or not progress bars are shown during conversion.
   * Progress bars are turned off by default.
   *
   * @param useProgressBars whether or not to show progress bars
   */
  @Option(
    names = {"-p", "--progress"},
    description = "Print progress bars during conversion",
    help = true
  )
  public void setProgressBars(boolean useProgressBars) {
    progressBars = useProgressBars;
  }

  /**
   * Configure whether to print version information and exit
   * without converting.
   *
   * @param versionOnly whether or not to print version information and exit
   */
  @Option(
      names = "--version",
      description = "Print version information and exit",
      help = true
  )
  public void setPrintVersionOnly(boolean versionOnly) {
    printVersion = versionOnly;
  }

  /**
   * Set the compression type for the output OME-TIFF. Defaults to LZW.
   * Valid types are defined in the CompressionType enum.
   *
   * @param compressionType compression type
   */
  @Option(
      names = "--compression",
      description = "Compression type for output OME-TIFF file " +
                    "(${COMPLETION-CANDIDATES}; default: ${DEFAULT-VALUE})",
      converter = CompressionTypeConverter.class,
      defaultValue = "LZW"
  )
  public void setCompression(CompressionType compressionType) {
    compression = compressionType;
  }

  /**
   * Set the compression options.
   *
   * When using the command line "--quality" option, the quality value will be
   * wrapped in a CodecOptions. The interpretation of the quality value
   * depends upon the selected compression type.
   *
   * This value currently only applies to "JPEG-2000 Lossy" compression,
   * and corresponds to the encoded bitrate in bits per pixel.
   * The quality is a floating point number and must be greater than 0.
   * A larger number implies less data loss but also larger file size.
   * By default, the quality is set to the largest positive finite double value.
   * This is equivalent to lossless compression; to see truly lossy compression,
   * the quality should be set to less than the bit depth of the input image.
   *
   * Options other than quality may be specified in this object, but their
   * interpretation will also depend upon the compression type selected.
   * Options that conflict with the input data (e.g. bits per pixel)
   * will be ignored.
   *
   * @param options compression options
   */
  @Option(
      names = "--quality",
      converter = CompressionQualityConverter.class,
      description = "Compression quality"
  )
  public void setCompressionOptions(CodecOptions options) {
    compressionOptions = options;
  }

  /**
   * Configure whether to write a pyramid OME-TIFF compatible with
   * Bio-Formats 6.x (the default), or a legacy pyramid TIFF compatible
   * with Bio-Formats 5.9.x.
   *
   * @param legacyTIFF true if a legacy pyramid TIFF should be written
   */
  @Option(
      names = "--legacy",
      description = "Write a Bio-Formats 5.9.x pyramid instead of OME-TIFF"
  )
  public void setLegacyTIFF(boolean legacyTIFF) {
    legacy = legacyTIFF;
  }

  /**
   * Configure whether to write one OME-TIFF per OME Image/Zarr group.
   *
   * @param split true if output should be split into one OME-TIFF per Image
   */
  @Option(
      names = "--split",
      description =
        "Split output into one OME-TIFF file per OME Image/Zarr group"
  )
  public void setSplitTIFFs(boolean split) {
    splitBySeries = split;
  }

  /**
   * Set the maximum number of workers to use for converting tiles.
   * Defaults to the number of detected CPUs.
   *
   * @param workers maximum worker count
   */
  @Option(
      names = "--max_workers",
      description = "Maximum number of workers (default: ${DEFAULT-VALUE})"
  )
  public void setMaxWorkers(int workers) {
    if (workers > 0) {
      maxWorkers = workers;
    }
  }

  /**
   * Write an RGB TIFF, if the input data contains a multiple of 3 channels
   * If RGB TIFFs are written, any channel metadata (names, wavelengths, etc.)
   * in the input data will be lost.
   *
   * @param isRGB true if an RGB TIFF should be written
   */
  @Option(
      names = "--rgb",
      description = "Attempt to write channels as RGB; " +
                    "channel count must be a multiple of 3"
  )
  public void setRGB(boolean isRGB) {
    rgb = isRGB;
  }

  /**
   * @return path to output data
   */
  public String getOutputPath() {
    return outputFilePath.toString();
  }

  /**
   * @return path to input data
   */
  public String getInputPath() {
    return inputDirectory.toString();
  }

  /**
   * @return slf4j logging level
   */
  public String getLogLevel() {
    return logLevel;
  }

  /**
   * @return true if progress bars are displayed
   */
  public boolean getProgressBars() {
    return progressBars;
  }

  // path and UUID for companion OME-XML file
  // only used with the --split option
  private String companionPath = null;
  private String companionUUID = null;

  /**
   * @return true if only version info is displayed
   */
  public boolean getPrintVersionOnly() {
    return printVersion;
  }

  /**
   * @return compression type
   */
  public CompressionType getCompression() {
    return compression;
  }

  /**
   * @return compression options
   */
  public CodecOptions getCompressionOptions() {
    return compressionOptions;
  }

  /**
   * @return true if a legacy pyramid TIFF will be written
   *         instead of pyramid OME-TIFF
   */
  public boolean getLegacyTIFF() {
    return legacy;
  }

  /**
   * @return true if output will be split into one OME-TIFF per image
   */
  public boolean getSplitTIFFs() {
    return splitBySeries;
  }

  /**
   * @return maximum number of worker threads
   */
  public int getMaxWorkers() {
    return maxWorkers;
  }

  /**
   * @return true if an RGB TIFF should be written
   */
  public boolean getRGB() {
    return rgb;
  }

  /**
   * Convert a pyramid based upon the provided command line arguments.
   * @param args command line arguments
   */
  public static void main(String[] args) {
    CommandLine.call(new PyramidFromDirectoryWriter(), args);
  }

  /**
   * Set a listener for tile processing events.
   * Intended to be used to show a status bar.
   *
   * @param listener a progress event listener
   */
  public void setProgressListener(IProgressListener listener) {
    progressListener = listener;
  }

  /**
   * Get the current listener for tile processing events.
   * If no listener was set, a no-op listener is returned.
   *
   * @return the current progress listener
   */
  public IProgressListener getProgressListener() {
    if (progressListener == null) {
      setProgressListener(new NoOpProgressListener());
    }
    return progressListener;
  }

  @Override
  public Void call() throws Exception {
    if (printVersion) {
      printVersion();
      return null;
    }

    if (progressBars) {
      setProgressListener(new ProgressBarListener(logLevel));
    }
    if (inputDirectory == null) {
      throw new IllegalArgumentException("Input directory not specified");
    }
    if (outputFilePath == null) {
      throw new IllegalArgumentException("Output path not specified");
    }

    // Resolve symlinks
    inputDirectory = inputDirectory.toRealPath();

    setupLogger();

    // we could support this case later, just keeping it simple for now
    if (splitBySeries && legacy) {
      throw new IllegalArgumentException("--split not supported with --legacy");
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
    return getZarr().resolve("OME").resolve(OMEXML_FILE);
  }

  /**
   * Get the root Zarr directory.
   *
   * @return Path representing the root Zarr directory
   */
  private Path getZarr() {
    return inputDirectory;
  }

  /**
   * Provide tile from the input folder.
   *
   * @param s current series
   * @param resolution the pyramid level, indexed from 0 (largest)
   * @param no the plane index
   * @param x the tile X index, from 0 to numberOfTilesX
   * @param y the tile Y index, from 0 to numberOfTilesY
   * @param region specifies the width and height to read;
                   if null, the whole tile is read
   * @return byte array containing the pixels for the tile
   */
  private byte[] getInputTileBytes(PyramidSeries s, int resolution,
      int no, int x, int y, Region region)
      throws FormatException, IOException
  {
    int[] pos = FormatTools.rasterToPosition(s.dimensionLengths, no);
    getProgressListener().notifyChunkStart(no, x, y, pos[0]);
    ResolutionDescriptor descriptor = s.resolutions.get(resolution);
    int realWidth = descriptor.tileSizeX;
    int realHeight = descriptor.tileSizeY;
    if (region != null) {
      realWidth = region.width;
      realHeight = region.height;
    }

    int[] gridPosition = new int[] {pos[2], pos[1], pos[0],
      y * descriptor.tileSizeY, x * descriptor.tileSizeX};
    int[] shape = new int[] {1, 1, 1, realHeight, realWidth};

    ZarrArray block = reader.openArray(descriptor.path);

    if (block == null) {
      throw new FormatException("Could not find block = " + descriptor.path +
        ", position = [" + pos[0] + ", " + pos[1] + ", " + pos[2] + "]");
    }

    byte[] tile = null;
    try {
      Object bytes = block.read(shape, gridPosition);
      if (bytes instanceof byte[]) {
        tile = (byte[]) bytes;
      }
      else if (bytes instanceof short[]) {
        tile = DataTools.shortsToBytes((short[]) bytes, s.littleEndian);
      }
      else if (bytes instanceof int[]) {
        tile = DataTools.intsToBytes((int[]) bytes, s.littleEndian);
      }
      else if (bytes instanceof long[]) {
        tile = DataTools.longsToBytes((long[]) bytes, s.littleEndian);
      }
      else if (bytes instanceof float[]) {
        tile = DataTools.floatsToBytes((float[]) bytes, s.littleEndian);
      }
      else if (bytes instanceof double[]) {
        tile = DataTools.doublesToBytes((double[]) bytes, s.littleEndian);
      }
    }
    catch (InvalidRangeException e) {
      throw new IOException("Could not read from " + descriptor.path, e);
    }

    return tile;
  }

  /**
   * Translate Zarr attributes to the current metadata store.
   */
  private void populateMetadata() throws IOException {
    if (plateData != null) {
      List<Map<String, Object>> acquisitions =
        (List<Map<String, Object>>) plateData.get("acquisitions");
      List<Map<String, Object>> columns =
        (List<Map<String, Object>>) plateData.get("columns");
      List<Map<String, Object>> rows =
        (List<Map<String, Object>>) plateData.get("rows");
      List<Map<String, Object>> wells =
        (List<Map<String, Object>>) plateData.get("wells");

      Map<String, Integer> rowLookup = new HashMap<String, Integer>();
      Map<String, Integer> colLookup = new HashMap<String, Integer>();

      for (int i=0; i<rows.size(); i++) {
        rowLookup.put(rows.get(i).get("name").toString(), i);
      }
      for (int i=0; i<columns.size(); i++) {
        Object name = columns.get(i).get("name");
        colLookup.put(getString(name), i);
      }

      metadata.setPlateID(MetadataTools.createLSID("Plate", 0), 0);
      metadata.setPlateName((String) plateData.get("name"), 0);
      metadata.setPlateRows(new PositiveInteger(rows.size()), 0);
      metadata.setPlateColumns(new PositiveInteger(columns.size()), 0);

      Map<String, Integer> acqLookup = new HashMap<String, Integer>();
      List<Integer> wsCounter = new ArrayList<Integer>();
      for (int i=0; i<acquisitions.size(); i++) {
        String acqID = MetadataTools.createLSID("PlateAcquisition", 0, i);
        metadata.setPlateAcquisitionID(acqID, 0, i);
        String plateAcqName = acquisitions.get(i).get("id").toString();
        metadata.setPlateAcquisitionName(plateAcqName, 0, i);
        wsCounter.add(0);
        acqLookup.put(plateAcqName, i);
      }

      int wsIndex = 0;
      for (int i=0; i<wells.size(); i++) {
        String well = (String) wells.get(i).get("path");
        Integer rowIndex = (Integer) wells.get(i).get("row_index");
        Integer colIndex = (Integer) wells.get(i).get("column_index");
        String[] path = well.split("/");

        if (rowIndex == null) {
          LOG.warn("Well {} row_index missing; attempting to calculate", i);
          rowIndex = rowLookup.get(path[path.length - 2]);
        }
        if (colIndex == null) {
          LOG.warn("Well {} column_index missing; attempting to calculate", i);
          colIndex = colLookup.get(path[path.length - 1]);
        }

        metadata.setWellID(MetadataTools.createLSID("Well", 0, i), 0, i);
        metadata.setWellColumn(new NonNegativeInteger(colIndex), 0, i);
        metadata.setWellRow(new NonNegativeInteger(rowIndex), 0, i);

        ZarrGroup wellGroup = getZarrGroup(well);

        Map<String, Object> wellAttr =
          (Map<String, Object>) wellGroup.getAttributes().get("well");
        List<Map<String, Object>> images =
          (List<Map<String, Object>>) wellAttr.get("images");
        for (int img=0; img<images.size(); img++) {
          String wsID = MetadataTools.createLSID("WellSample", 0, i, img);
          metadata.setWellSampleID(wsID, 0, i, img);
          metadata.setWellSampleIndex(
            new NonNegativeInteger(wsIndex), 0, i, img);

          String imageID = MetadataTools.createLSID("Image", wsIndex);
          metadata.setWellSampleImageRef(imageID, 0, i, img);
          int acquisition =
            acqLookup.get(images.get(img).get("acquisition").toString());
          int acqIndex = wsCounter.get(acquisition);
          metadata.setPlateAcquisitionWellSampleRef(
            wsID, 0, acquisition, acqIndex);
          wsCounter.set(acquisition, acqIndex + 1);

          metadata.setImageID(imageID, wsIndex);
          metadata.setPixelsID(
            MetadataTools.createLSID("Pixels", wsIndex), wsIndex);

          String imgName = (String) images.get(img).get("path");
          String imgPath = well + "/" + imgName;
          metadata.setImageName(imgName, wsIndex);

          ZarrGroup imgGroup = getZarrGroup(imgPath);
          ZarrArray imgArray = imgGroup.openArray("0");
          int[] dims = imgArray.getShape();

          String order = "XYZCT";
          int cIndex = order.length() - order.indexOf("C") - 1;
          int zIndex = order.length() - order.indexOf("Z") - 1;
          int tIndex = order.length() - order.indexOf("T") - 1;
          int c = dims[cIndex];
          PixelType type = getPixelType(imgArray.getDataType());

          boolean bigEndian = imgArray.getByteOrder() == ByteOrder.BIG_ENDIAN;

          metadata.setPixelsBigEndian(bigEndian, wsIndex);
          metadata.setPixelsType(type, wsIndex);
          metadata.setPixelsSizeX(
            new PositiveInteger(dims[dims.length - 1]), wsIndex);
          metadata.setPixelsSizeY(
            new PositiveInteger(dims[dims.length - 2]), wsIndex);
          metadata.setPixelsSizeZ(new PositiveInteger(dims[zIndex]), wsIndex);
          metadata.setPixelsSizeC(new PositiveInteger(c), wsIndex);
          metadata.setPixelsSizeT(new PositiveInteger(dims[tIndex]), wsIndex);
          try {
            metadata.setPixelsDimensionOrder(
              DimensionOrder.fromString(order), wsIndex);
          }
          catch (EnumerationException e) {
            LOG.warn("Could not save dimension order", e);
          }

          for (int ch=0; ch<c; ch++) {
            metadata.setChannelID(
              MetadataTools.createLSID("Channel", wsIndex, ch), wsIndex, ch);
            metadata.setChannelSamplesPerPixel(
              new PositiveInteger(1), wsIndex, ch);
          }

          wsIndex++;
        }
      }
    }
  }

  private PixelType getPixelType(DataType type) {
    switch (type) {
      case i1:
        return PixelType.INT8;
      case u1:
        return PixelType.UINT8;
      case i2:
        return PixelType.INT16;
      case u2:
        return PixelType.UINT16;
      case i4:
        return PixelType.INT32;
      case u4:
        return PixelType.UINT32;
      case f4:
        return PixelType.FLOAT;
      case f8:
        return PixelType.DOUBLE;
      default:
        throw new IllegalArgumentException("Unsupported pixel type: " + type);
    }
  }

  private ZarrGroup getZarrGroup(String path) throws IOException {
    return ZarrGroup.open(inputDirectory.resolve(path).toString());
  }

  private int getSubgroupCount(String path) throws IOException {
    return getZarrGroup(path).getGroupKeys().size();
  }

  /**
   * Calculate the number of series.
   *
   * @return number of series
   */
  private int getSeriesCount() throws IOException {
    Set<String> groupKeys = reader.getGroupKeys();
    groupKeys.remove("OME");
    int groupKeyCount = groupKeys.size();
    LOG.debug("getSeriesCount:");
    LOG.debug("  plateData = {}", plateData);
    LOG.debug("  group key count = {}", groupKeyCount);
    if (plateData != null) {
      int count = 0;
      List<Map<String, Object>> wells =
        (List<Map<String, Object>>) plateData.get("wells");
      for (Map<String, Object> well : wells) {
        count += getSubgroupCount((String) well.get("path"));
      }
      LOG.debug("  returning plate-based series count = {}", count);
      return count;
    }
    return groupKeyCount;
  }

  /**
   * Calculate the number of resolutions for the given series based upon
   * the number of directories in the base input directory.
   *
   * @param s current series
   */
  private void findNumberOfResolutions(PyramidSeries s) throws IOException {
    if (plateData != null) {
      List<Map<String, Object>> wells =
        (List<Map<String, Object>>) plateData.get("wells");
      int index = 0;
      for (Map<String, Object> well : wells) {
        int fields = getSubgroupCount((String) well.get("path"));
        if (index + fields > s.index) {
          s.path = well.get("path") + "/" + (s.index - index);
          break;
        }
        index += fields;
      }
    }
    else {
      s.path = String.valueOf(s.index);
    }

    ZarrGroup seriesGroup = getZarrGroup(s.path);
    if (seriesGroup == null) {
      throw new IOException("Expected series " + s.index + " not found");
    }

    // use multiscales metadata if it exists, to distinguish between
    // resolutions and labels
    // if no multiscales metadata (older dataset?), assume no labels
    // and just use the path listing length
    Map<String, Object> seriesAttributes = seriesGroup.getAttributes();
    List<Map<String, Object>> multiscales =
      (List<Map<String, Object>>) seriesAttributes.get("multiscales");
    if (multiscales != null && multiscales.size() > 0) {
      List<Map<String, Object>> datasets =
        (List<Map<String, Object>>) multiscales.get(0).get("datasets");
      if (datasets != null) {
        s.numberOfResolutions = datasets.size();
      }
    }

    if (s.numberOfResolutions == 0) {
      s.numberOfResolutions = seriesGroup.getArrayKeys().size();
    }
  }

  /**
   * Set up the TIFF writer with all necessary metadata.
   * After this method is called, image data can be written.
   */
  public void initialize()
    throws FormatException, IOException, DependencyException
  {
    createReader();

    if (reader == null) {
      throw new FormatException("Could not create a reader");
    }

    Map<String, Object> attributes = reader.getAttributes();
    Integer layoutVersion = (Integer) attributes.get("bioformats2raw.layout");
    if (layoutVersion == null) {
      LOG.warn("Layout version not recorded; may be unsupported");
    }
    else if (layoutVersion != 3) {
      throw new FormatException("Unsupported version: " + layoutVersion);
    }

    plateData = (Map<String, Object>) attributes.get("plate");

    LOG.info("Creating tiled pyramid file {}", this.outputFilePath);

    OMEXMLService service = getService();
    if (service != null) {
      Path omexml = getOMEXMLFile();
      String xml = null;
      if (omexml != null && Files.exists(omexml)) {
        xml = DataTools.readFile(omexml.toString());
      }
      try {
        if (xml != null) {
          metadata = (OMEPyramidStore) service.createOMEXMLMetadata(xml);

          OMEXMLMetadataRoot root = (OMEXMLMetadataRoot) metadata.getRoot();
          for (int image = 0; image<root.sizeOfImageList(); image++) {
            Pixels pixels = root.getImage(image).getPixels();
            pixels.setMetadataOnly(null);

            while (pixels.sizeOfTiffDataList() > 0) {
              pixels.removeTiffData(pixels.getTiffData(0));
            }
          }
        }
        else {
          metadata = (OMEPyramidStore) service.createOMEXMLMetadata();
          populateMetadata();
        }
      }
      catch (ServiceException e) {
        throw new FormatException("Could not parse OME-XML", e);
      }
    }

    int seriesCount = getSeriesCount();

    if (seriesCount < 1) {
      throw new FormatException("Found no images to convert.  Corrupt input?");
    }

    if (seriesCount > 1 && legacy) {
      LOG.warn("Omitting {} series due to legacy output", seriesCount - 1);
      seriesCount = 1;
    }

    int totalPlanes = 0;
    seriesPaths = new ArrayList<Path>(seriesCount);
    for (int seriesIndex=0; seriesIndex<seriesCount; seriesIndex++) {
      PyramidSeries s = new PyramidSeries();
      s.index = seriesIndex;
      findNumberOfResolutions(s);

      s.z = metadata.getPixelsSizeZ(seriesIndex).getNumberValue().intValue();
      s.c = metadata.getPixelsSizeC(seriesIndex).getNumberValue().intValue();
      s.t = metadata.getPixelsSizeT(seriesIndex).getNumberValue().intValue();
      s.dimensionOrder =
        metadata.getPixelsDimensionOrder(seriesIndex).toString();
      s.planeCount = s.z * s.t;

      // Zarr format allows both little and big endian order
      s.littleEndian = !metadata.getPixelsBigEndian(seriesIndex);

      s.pixelType = FormatTools.pixelTypeFromString(
            metadata.getPixelsType(seriesIndex).getValue());

      if (seriesIndex > 0 && s.littleEndian != series.get(0).littleEndian) {
        // always warn on endian mismatches
        // mismatch is only fatal if pixel type is neither INT8 nor UINT8
        String msg = String.format("Endian mismatch in series {} (expected {}",
          seriesIndex, series.get(0).littleEndian);
        if (FormatTools.getBytesPerPixel(s.pixelType) > 1) {
          throw new FormatException(msg);
        }
        LOG.warn(msg);
      }

      s.dimensionLengths[s.dimensionOrder.indexOf("Z") - 2] = s.z;
      s.dimensionLengths[s.dimensionOrder.indexOf("T") - 2] = s.t;
      s.dimensionLengths[s.dimensionOrder.indexOf("C") - 2] = s.c;

      int rgbChannels = 1;
      int effectiveChannels = s.c;

      // --rgb flag only respected if the number of channels in the source data
      // is a multiple of 3
      // this assumes that channels should be grouped by 3s (not 2s or 4s)
      // into RGB planes
      // this could be made configurable later?
      s.rgb = rgb && (s.c % 3 == 0);
      if (s.rgb) {
        rgbChannels = 3;
        effectiveChannels = s.c / rgbChannels;
        LOG.debug("Merging {} original channels into {} RGB channels",
          s.c, effectiveChannels);

        OMEXMLMetadataRoot root = (OMEXMLMetadataRoot) metadata.getRoot();
        Pixels pixels = root.getImage(seriesIndex).getPixels();
        for (int index=pixels.sizeOfChannelList()-1; index>0; index--) {
          if (index % rgbChannels == 0) {
            continue;
          }
          Channel ch = pixels.getChannel(index);
          pixels.removeChannel(ch);
        }
        for (int index=0; index<pixels.sizeOfChannelList(); index++) {
          Channel channel = pixels.getChannel(index);
          channel.setSamplesPerPixel(new PositiveInteger(rgbChannels));
          if (channel.getColor() != null) {
            LOG.warn("Removing channel color");
            channel.setColor(null);
          }
          if (channel.getEmissionWavelength() != null) {
            LOG.warn("Removing channel emission wavelength");
            channel.setEmissionWavelength(null);
          }
          if (channel.getExcitationWavelength() != null) {
            LOG.warn("Removing channel excitation wavelength");
            channel.setEmissionWavelength(null);
          }
          if (channel.getLightPath() != null) {
            LOG.warn("Removing channel light path");
            channel.setLightPath(null);
          }
          if (channel.getLightSourceSettings() != null) {
            LOG.warn("Removing channel light source settings");
            channel.setLightSourceSettings(null);
          }
          FilterSet filterSet = channel.getLinkedFilterSet();
          if (filterSet != null) {
            LOG.warn("Removing channel filter set");
            channel.unlinkFilterSet(filterSet);
          }
          if (channel.getName() != null) {
            LOG.warn("Removing channel name");
            channel.setName(null);
          }
        }

        // RGB data needs to have XYC* dimension order
        if (!s.dimensionOrder.startsWith("XYC")) {
          if (s.dimensionOrder.indexOf("Z") < s.dimensionOrder.indexOf("T")) {
            s.dimensionOrder = "XYCZT";
          }
          else {
            s.dimensionOrder = "XYCTZ";
          }
        }
      }
      else if (rgb) {
        LOG.warn(
          "Ignoring --rgb flag; channel count {} is not a multiple of 3", s.c);
      }

      s.planeCount *= effectiveChannels;

      s.describePyramid(reader, metadata);

      metadata.setTiffDataIFD(new NonNegativeInteger(totalPlanes), s.index, 0);
      metadata.setTiffDataPlaneCount(
        new NonNegativeInteger(s.planeCount), s.index, 0);

      for (ResolutionDescriptor descriptor : s.resolutions) {
        LOG.info("Adding metadata for resolution: {}",
          descriptor.resolutionNumber);

        if (descriptor.resolutionNumber == 0) {
          MetadataTools.populateMetadata(
            this.metadata, seriesIndex, null, s.littleEndian, s.dimensionOrder,
            FormatTools.getPixelTypeString(s.pixelType),
            descriptor.sizeX, descriptor.sizeY, s.z, s.c, s.t,
            rgbChannels);
        }
        else {
          if (legacy) {
            MetadataTools.populateMetadata(this.metadata,
              descriptor.resolutionNumber, null, s.littleEndian,
              s.dimensionOrder, FormatTools.getPixelTypeString(s.pixelType),
              descriptor.sizeX, descriptor.sizeY,
              s.z, s.c, s.t, rgbChannels);
          }
          else {
            metadata.setResolutionSizeX(new PositiveInteger(
              descriptor.sizeX), seriesIndex, descriptor.resolutionNumber);
            metadata.setResolutionSizeY(new PositiveInteger(
              descriptor.sizeY), seriesIndex, descriptor.resolutionNumber);
          }
        }
      }

      series.add(s);
      totalPlanes += s.planeCount;

      if (splitBySeries) {
        String basePath = getOutputPathPrefix();
        // append the series index and file extension
        basePath += "_s";
        seriesPaths.add(Paths.get(basePath + s.index + ".ome.tiff"));
        // generate one UUID per file
        s.uuid = "urn:uuid:" + UUID.randomUUID().toString();
      }
      else {
        seriesPaths.add(outputFilePath);
        // use the same UUID everywhere since we're only writing one file
        if (seriesIndex == 0) {
          s.uuid = "urn:uuid:" + UUID.randomUUID().toString();
        }
        else {
          s.uuid = series.get(0).uuid;
        }
      }
    }

    populateTiffData();
    populateOriginalMetadata(service);

    if (splitBySeries) {
      // splitting into separate OME-TIFFs results in a companion OME-XML file
      // the OME-TIFFs then use the BinaryOnly element to reference the OME-XML
      // for large plates in particular, this is useful as it reduces the
      // OME-TIFF file size

      companionPath = getOutputPathPrefix() + ".companion.ome";
      companionUUID = "urn:uuid:" + UUID.randomUUID().toString();

      try {
        metadata.setUUID(companionUUID);
        String omexml = service.getOMEXML(metadata);
        Files.write(Paths.get(companionPath),
          omexml.getBytes(Constants.ENCODING));
      }
      catch (ServiceException e) {
        throw new FormatException("Could not get OME-XML", e);
      }
    }

    for (Path p : seriesPaths) {
      writeTIFFHeader(p.toString());
    }
  }

  /**
   * Remove the [.ome].tif[f] suffix from the output file path, if present.
   *
   * @return output file path without OME-TIFF extension
   */
  private String getOutputPathPrefix() {
    String basePath = outputFilePath.toString();
    if (basePath.toLowerCase().endsWith(".tif") ||
      basePath.toLowerCase().endsWith(".tiff"))
    {
      basePath = basePath.substring(0, basePath.lastIndexOf("."));
      if (basePath.toLowerCase().endsWith(".ome")) {
        basePath = basePath.substring(0, basePath.lastIndexOf("."));
      }
    }
    return basePath;
  }

  private void writeTIFFHeader(String output) throws IOException {
    try (RandomAccessOutputStream out = new RandomAccessOutputStream(output)) {
      try (TiffSaver w = createTiffSaver(out, output)) {
        w.writeHeader();
      }
    }
    tiffSavers.remove(output);
  }

  private TiffSaver createTiffSaver(RandomAccessOutputStream out, String file) {
    if (tiffSavers.containsKey(file)) {
      return tiffSavers.get(file);
    }
    TiffSaver w = new TiffSaver(out, file);
    w.setBigTiff(true);
    // assumes all series have same endian setting
    // series with opposite endianness are logged above
    w.setLittleEndian(series.get(0).littleEndian);
    tiffSavers.put(file, w);
    return w;
  }

  private String getSeriesPathName(PyramidSeries s) {
    return seriesPaths.get(s.index).toString();
  }

   //* Conversion */

  /**
   * Writes all image data to the initialized TIFF writer.
   */
  public void convertToPyramid()
    throws FormatException, IOException,
      InterruptedException, DependencyException
  {
    for (PyramidSeries s : series) {
      convertPyramid(s);
    }
    StopWatch t0 = new Slf4JStopWatch("writeIFDs");
    writeIFDs();
    t0.stop();
    binaryOnly = null;
  }

  private void convertPyramid(PyramidSeries s)
    throws FormatException, IOException,
      InterruptedException, DependencyException
  {
    getProgressListener().notifySeriesStart(s.index);

    // convert every resolution in the pyramid
    s.ifds = new IFDList[s.numberOfResolutions];
    for (int resolution=0; resolution<s.numberOfResolutions; resolution++) {
      s.ifds[resolution] = new IFDList();
      for (int plane=0; plane<s.planeCount; plane++) {
        IFD ifd = makeIFD(s, resolution, plane);
        s.ifds[resolution].add(ifd);
      }
    }

    int rgbChannels = s.rgb ? 3 : 1;
    int bytesPerPixel = FormatTools.getBytesPerPixel(s.pixelType);
    for (int resolution=0; resolution<s.numberOfResolutions; resolution++) {
      executor = new ThreadPoolExecutor(
        maxWorkers, maxWorkers, 0L, TimeUnit.MILLISECONDS, tileQueue);

      try {
        LOG.info("Converting resolution #{}", resolution);
        ResolutionDescriptor descriptor = s.resolutions.get(resolution);
        int tileCount = descriptor.numberOfTilesY * descriptor.numberOfTilesX;
        int totalTileCount = tileCount * s.planeCount;

        getProgressListener().notifyResolutionStart(resolution, totalTileCount);

        int plane = 0;
        for (int t=0; t<s.t; t++) {
          for (int c=0; c<(s.c / rgbChannels); c++) {
            for (int z=0; z<s.z; z++, plane++) {
              int tileIndex = 0;
              // if the resolution has already been calculated,
              // just read each tile from disk and store in the OME-TIFF
              for (int y=0; y<descriptor.numberOfTilesY; y++) {
                for (int x=0; x<descriptor.numberOfTilesX; x++, tileIndex++) {
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

                  StopWatch t0 = new Slf4JStopWatch("getInputTileBytes");
                  byte[] packedTileBytes = null;
                  try {
                    for (int ch=0; ch<rgbChannels; ch++) {
                      // assumes TCZYX order consistent with bioformats2raw
                      int planeIndex = FormatTools.positionToRaster(
                        s.dimensionLengths,
                        new int[] {z, c * rgbChannels + ch, t});
                      byte[] componentBytes = getInputTileBytes(
                        s, resolution, planeIndex, x, y, region);
                      if (rgbChannels == 1) {
                        packedTileBytes = componentBytes;
                      }
                      else {
                        if (ch == 0) {
                          packedTileBytes =
                            new byte[componentBytes.length * rgbChannels];
                        }
                        // unpack componentBytes into packedTileBytes
                        int pixels = componentBytes.length / bytesPerPixel;
                        for (int pixel=0; pixel<pixels; pixel++) {
                          int srcIndex = pixel * bytesPerPixel;
                          int destIndex =
                            bytesPerPixel * (pixel * rgbChannels + ch);
                          for (int b=0; b<bytesPerPixel; b++) {
                            packedTileBytes[destIndex + b] =
                              componentBytes[srcIndex + b];
                          }
                        }
                      }
                    }
                  }
                  finally {
                    t0.stop();
                  }

                  final int currentIndex = tileIndex;
                  final int currentPlane = plane;
                  final int currentResolution = resolution;
                  final int xx = x;
                  final int yy = y;
                  final byte[] tileBytes = packedTileBytes;
                  executor.execute(() -> {
                    Slf4JStopWatch t1 = new Slf4JStopWatch("writeTile");
                    try {
                      if (tileBytes != null) {
                        if (region.width == descriptor.tileSizeX &&
                          region.height == descriptor.tileSizeY)
                        {
                          writeTile(s, currentPlane, tileBytes,
                            currentIndex, currentResolution, xx, yy);
                        }
                        else {
                          // padded tile, use descriptor X and Y tile size
                          int tileX = descriptor.tileSizeX;
                          int tileY = descriptor.tileSizeY;
                          int pixelWidth = bytesPerPixel * rgbChannels;
                          byte[] realTile =
                            new byte[tileX * tileY * pixelWidth];
                          int totalRows = region.height;
                          int inRowLen = region.width * pixelWidth;
                          int outRowLen = tileX * pixelWidth;
                          for (int row=0; row<totalRows; row++) {
                            System.arraycopy(tileBytes, row * inRowLen,
                              realTile, row * outRowLen, inRowLen);
                          }
                          writeTile(s, currentPlane, realTile,
                            currentIndex, currentResolution, xx, yy);
                        }
                      }
                    }
                    catch (FormatException|IOException e) {
                      LOG.error(
                        "Failed to write tile in series {} resolution {}",
                        s.index, currentResolution, e);
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
        getProgressListener().notifyResolutionEnd(resolution);
      }
    }
    getProgressListener().notifySeriesEnd(s.index);
  }

  private void writeIFDs() throws FormatException, IOException {
    StopWatch t0 = new Slf4JStopWatch("subifds");
    Map<String, Long> firstIFDOffsets = new HashMap<String, Long>();

    // write sub-IFDs for every series first
    long[][][] subs = new long[series.size()][][];
    for (PyramidSeries s : series) {
      StopWatch t1 = new Slf4JStopWatch("subifd-" + s.index);
      String path = getSeriesPathName(s);
      try (RandomAccessOutputStream out = new RandomAccessOutputStream(path)) {
        out.order(s.littleEndian);
        out.seek(out.length());
        if (!firstIFDOffsets.containsKey(path)) {
          firstIFDOffsets.put(path, out.getFilePointer());
        }

        if (legacy) {
          for (int res=0; res<s.numberOfResolutions; res++) {
            for (int plane=0; plane<s.planeCount; plane++) {
              boolean last = (res == s.numberOfResolutions - 1) &&
                (plane == s.planeCount - 1);
              writeIFD(out, s, res, plane, !last);
            }
          }
          out.seek(FIRST_IFD_OFFSET);
          out.writeLong(firstIFDOffsets.get(path));
        }
        else {
          subs[s.index] = new long[s.planeCount][s.numberOfResolutions - 1];
          for (int plane=0; plane<s.planeCount; plane++) {
            for (int r=1; r<s.numberOfResolutions; r++) {
              subs[s.index][plane][r - 1] = out.getFilePointer();
              writeIFD(out, s, r, plane, r < s.numberOfResolutions - 1);
            }
          }
        }
      }
      tiffSavers.remove(path);
      t1.stop();
    }
    t0.stop();

    t0 = new Slf4JStopWatch("fullResolutionIFDs");
    // now write the full resolution IFD for each series
    if (!legacy) {
      firstIFDOffsets.clear();
      for (PyramidSeries s : series) {
        StopWatch t1 = new Slf4JStopWatch("fullResolution-" + s.index);
        String path = getSeriesPathName(s);
        try (RandomAccessOutputStream out =
          new RandomAccessOutputStream(path))
        {
          out.order(s.littleEndian);
          out.seek(out.length());

          if (!firstIFDOffsets.containsKey(path)) {
            firstIFDOffsets.put(path, out.getFilePointer());
          }

          for (int plane=0; plane<s.planeCount; plane++) {
            s.ifds[0].get(plane).put(IFD.SUB_IFD, subs[s.index][plane]);
            boolean overwrite = plane < s.planeCount - 1;
            if (!splitBySeries) {
              overwrite = overwrite || s.index < series.size() - 1;
            }
            writeIFD(out, s, 0, plane, overwrite);
          }
          out.seek(FIRST_IFD_OFFSET);
          out.writeLong(firstIFDOffsets.get(path));
        }
        tiffSavers.remove(path);
        t1.stop();
      }
    }
    t0.stop();
  }

  /**
   * Overwrite an IFD offset at the given pointer.
   * The output stream should be positioned to the new IFD offset
   * before this method is called.
   *
   * @param outStream open output file stream
   * @param offsetPointer pointer to the IFD offset that will be overwritten
   * @throws IOException
   */
  private void overwriteNextOffset(
    RandomAccessOutputStream outStream, long offsetPointer) throws IOException
  {
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
   * Create an IFD for the given plane in the given resolution.
   * All relevant tags are filled in; sub-IFD and tile data are
   * filled with placeholders.
   *
   * @param s current series
   * @param resolution the resolution index for the new IFD
   * @param plane the plane index for the new IFD
   * @return an IFD that is ready to be filled with tile data
   */
  private IFD makeIFD(PyramidSeries s, int resolution, int plane)
    throws FormatException, DependencyException
  {
    IFD ifd = new IFD();
    ifd.put(IFD.LITTLE_ENDIAN, s.littleEndian);
    ResolutionDescriptor descriptor = s.resolutions.get(resolution);
    ifd.put(IFD.IMAGE_WIDTH, (long) descriptor.sizeX);
    ifd.put(IFD.IMAGE_LENGTH, (long) descriptor.sizeY);
    ifd.put(IFD.TILE_WIDTH, descriptor.tileSizeX);
    ifd.put(IFD.TILE_LENGTH, descriptor.tileSizeY);
    ifd.put(IFD.COMPRESSION, compression.getTIFFCompression().getCode());

    ifd.put(IFD.PLANAR_CONFIGURATION, 1);

    int sampleFormat = 1;
    if (FormatTools.isFloatingPoint(s.pixelType)) {
      sampleFormat = 3;
    }
    else if (FormatTools.isSigned(s.pixelType)) {
      sampleFormat = 2;
    }

    ifd.put(IFD.SAMPLE_FORMAT, sampleFormat);

    int[] bps = new int[s.rgb ? 3 : 1];
    Arrays.fill(bps, FormatTools.getBytesPerPixel(s.pixelType) * 8);
    ifd.put(IFD.BITS_PER_SAMPLE, bps);

    ifd.put(IFD.PHOTOMETRIC_INTERPRETATION,
      s.rgb ? PhotoInterp.RGB.getCode() : PhotoInterp.BLACK_IS_ZERO.getCode());
    ifd.put(IFD.SAMPLES_PER_PIXEL, bps.length);

    if (legacy) {
      ifd.put(IFD.SOFTWARE, "Faas-raw2ometiff");
    }
    else {
      ifd.put(IFD.SOFTWARE, FormatTools.CREATOR);

      if (resolution == 0) {
        ifd.put(IFD.SUB_IFD, (long) 0);
      }
      else {
        ifd.put(IFD.NEW_SUBFILE_TYPE, 1);
      }
    }

    // only write the OME-XML to the first full-resolution IFD
    if (resolution == 0 && plane == 0) {
      if (splitBySeries) {
        // if each series is in a separate OME-TIFF, store BinaryOnly OME-XML
        // that references the companion OME-XML file

        String omexml = getBinaryOnlyOMEXML(s);
        ifd.put(IFD.IMAGE_DESCRIPTION, omexml);
      }
      else if (s.index == 0) {
        // if everything is in one OME-TIFF file, store the complete OME-XML
        try {
          metadata.setUUID(s.uuid);
          OMEXMLService service = getService();
          String omexml = service.getOMEXML(metadata);
          ifd.put(IFD.IMAGE_DESCRIPTION, omexml);
        }
        catch (ServiceException e) {
          throw new FormatException("Could not get OME-XML", e);
        }
      }
    }

    int tileCount = descriptor.numberOfTilesX * descriptor.numberOfTilesY;
    ifd.put(IFD.TILE_BYTE_COUNTS, new long[tileCount]);
    ifd.put(IFD.TILE_OFFSETS, new long[tileCount]);

    ifd.put(IFD.RESOLUTION_UNIT, 3);
    ifd.put(IFD.X_RESOLUTION,
      getPhysicalSize(metadata.getPixelsPhysicalSizeX(s.index)));
    ifd.put(IFD.Y_RESOLUTION,
      getPhysicalSize(metadata.getPixelsPhysicalSizeY(s.index)));

    return ifd;
  }

  private TiffRational getPhysicalSize(Length size) {
    if (size == null || size.value(UNITS.MICROMETER) == null) {
      return new TiffRational(0, 1000);
    }
    Double physicalSize = size.value(UNITS.MICROMETER).doubleValue();
    if (physicalSize.doubleValue() != 0) {
      physicalSize = 1d / physicalSize;
    }

    return new TiffRational((long) (physicalSize * 1000 * 10000), 1000);
  }

  /**
   * Write a single tile to the writer's current resolution.
   *
   * @param s current series
   * @param imageNumber the plane number in the resolution
   * @param buffer the array containing the tile's pixel data
   * @param tileIndex index of the tile to be written in XY space
   * @param resolution resolution index of the tile
   * @param x tile index along X
   * @param y tile index along Y
   */
  private void writeTile(PyramidSeries s,
      Integer imageNumber, byte[] buffer, int tileIndex, int resolution,
      int x, int y)
      throws FormatException, IOException
  {
    LOG.debug("Writing series: {}, image: {}, tileIndex: {}",
      s.index, imageNumber, tileIndex);

    IFD ifd = s.ifds[resolution].get(imageNumber);
    TiffCompression tiffCompression = compression.getTIFFCompression();
    CodecOptions options =
      tiffCompression.getCompressionCodecOptions(ifd, compressionOptions);

    // buffer has been padded to full tile width before calling writeTile
    // but is not necessarily full tile height (if in the bottom row)
    int bpp = FormatTools.getBytesPerPixel(s.pixelType);
    options.width = (int) ifd.getTileWidth();
    options.channels = s.rgb ? 3 : 1;
    options.height = buffer.length / (options.width * bpp * options.channels);
    options.bitsPerSample = bpp * 8;

    byte[] realTile = tiffCompression.compress(buffer, options);

    writeToDisk(s, realTile, tileIndex, resolution, imageNumber);
    int z = s.getZCTCoords(imageNumber)[0];
    getProgressListener().notifyChunkEnd(imageNumber, x, y, z);
  }

  /**
   * Write a pre-compressed buffer corresponding to the
   * given IFD and tile index.
   *
   * @param s current series
   * @param realTile array of compressed bytes representing the tile
   * @param tileIndex index into the array of tile offsets
   * @param resolution resolution index of the tile
   * @param imageNumber image index of the tile
   */
  private synchronized void writeToDisk(
      PyramidSeries s,
      byte[] realTile, int tileIndex,
      int resolution, int imageNumber)
      throws FormatException, IOException
  {
    IFD ifd = s.ifds[resolution].get(imageNumber);

    // do not use ifd.getStripByteCounts() or ifd.getStripOffsets() here
    // as both can return values other than what is in the IFD
    long[] offsets = ifd.getIFDLongArray(IFD.TILE_OFFSETS);
    long[] byteCounts = ifd.getIFDLongArray(IFD.TILE_BYTE_COUNTS);
    byteCounts[tileIndex] = (long) realTile.length;

    String file = getSeriesPathName(s);
    try (RandomAccessOutputStream out = new RandomAccessOutputStream(file)) {
      out.seek(out.length());
      offsets[tileIndex] = out.getFilePointer();

      LOG.debug("    writing {} compressed bytes to {} at {}",
        realTile.length, file, out.getFilePointer());

      out.write(realTile);
    }

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
   * Write the IFD for the given resolution and plane.
   *
   * @param outStream open output file stream
   * @param s current series
   * @param resolution the resolution index
   * @param plane the plane index
   * @param overwrite true unless this is the last IFD in the list
   */
  private void writeIFD(
    RandomAccessOutputStream outStream,
    PyramidSeries s, int resolution, int plane, boolean overwrite)
    throws FormatException, IOException
  {
    TiffSaver writer = createTiffSaver(outStream, getSeriesPathName(s));
    int ifdSize = getIFDSize(s.ifds[resolution].get(plane));
    long offsetPointer = outStream.getFilePointer() + ifdSize;
    writer.writeIFD(s.ifds[resolution].get(plane), 0);
    if (overwrite) {
      overwriteNextOffset(outStream, offsetPointer);
    }
  }

  /**
   * Create a reader for the chosen input directory.
   * If the input directory contains a Zarr group, a Zarr reader is used.
   * If an appropriate reader cannot be found, the reader will remain null.
   */
  private void createReader() throws IOException {
    Path zarr = getZarr();
    LOG.debug("attempting to open {}", zarr);
    if (Files.exists(zarr)) {
      LOG.debug("  zarr directory exists");
      reader = ZarrGroup.open(zarr.toString());
    }
  }

  /**
   * Set up the root logger, turning on debug logging if appropriate.
   */
  private void setupLogger() {
    ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)
      LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.toLevel(logLevel));
  }

  /**
   * Print versions for raw2ometiff and associated Bio-Formats dependency.
   */
  private void printVersion() {
    String version = Optional.ofNullable(
      this.getClass().getPackage().getImplementationVersion()
      ).orElse("development");
    System.out.println("Version = " + version);
    System.out.println("Bio-Formats version = " + FormatTools.VERSION);
  }

  private void populateTiffData() {
    for (PyramidSeries s : series) {
      metadata.setUUIDFileName(
        Paths.get(getSeriesPathName(s)).getFileName().toString(), s.index, 0);
      metadata.setUUIDValue(s.uuid, s.index, 0);
      if (splitBySeries) {
        metadata.setTiffDataIFD(new NonNegativeInteger(0), s.index, 0);
      }
    }
  }

  /**
   * Get a BinaryOnly OME-XML string for the given OME-TIFF path.
   * When datasets are split across multiple OME-TIFF files,
   * a companion OME-XML file is used which requires each OME-TIFF
   * to have BinaryOnly OME-XML that references the companion OME-XML file.
   *
   * @param s pyramid series for UUID retrieval
   * @return corresponding BinaryOnly OME-XML string
   */
  private String getBinaryOnlyOMEXML(PyramidSeries s) {
    try {
      OMEXMLService service = getService();
      if (binaryOnly == null) {
        binaryOnly = (OMEPyramidStore) service.createOMEXMLMetadata();

        Path companion = Paths.get(companionPath);
        binaryOnly.setBinaryOnlyMetadataFile(
          companion.getName(companion.getNameCount() - 1).toString());
        binaryOnly.setBinaryOnlyUUID(companionUUID);
      }
      binaryOnly.setUUID(s.uuid);
      return service.getOMEXML(binaryOnly);
    }
    catch (DependencyException | ServiceException e) {
      LOG.warn("Could not create OME-XML for " + getSeriesPathName(s), e);
    }
    return null;
  }

  /**
   * Use the given service to create original metadata annotations
   * based upon the dataset's JSON metadata file (if present).
   *
   * @param service to use for creating original metadata annotations
   * @throws IOException
   */
  private void populateOriginalMetadata(OMEXMLService service)
    throws IOException
  {
    Path metadataFile = getMetadataFile();
    Hashtable<String, Object> originalMeta = new Hashtable<String, Object>();
    if (metadataFile != null && Files.exists(metadataFile)) {
      String jsonMetadata = DataTools.readFile(metadataFile.toString());
      JSONObject json = new JSONObject(jsonMetadata);

      parseJSONValues(json, originalMeta, "");

      service.populateOriginalMetadata(metadata, originalMeta);
    }
  }

  /**
   * Translate JSON objects to a set of key/value pairs.
   *
   * @param root JSON object
   * @param originalMeta hashtable to store key/value pairs
   * @param prefix key prefix, used to preserve JSON hierarchy
   */
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

  private String getString(Object attr) {
    if (attr == null) {
      return null;
    }
    if (attr instanceof Double) {
      return String.valueOf(((Double) attr).intValue());
    }
    return attr.toString();
  }

}
