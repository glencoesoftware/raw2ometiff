/**
 * Copyright (c) 2020 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.raw2ometiff.test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.glencoesoftware.bioformats2raw.Converter;
import com.glencoesoftware.pyramid.CompressionType;
import com.glencoesoftware.pyramid.PyramidFromDirectoryWriter;

import loci.common.DataTools;
import loci.common.services.ServiceFactory;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.ImageReader;
import loci.formats.ome.OMEXMLMetadata;
import loci.formats.services.OMEXMLService;
import loci.formats.tiff.IFD;
import loci.formats.tiff.IFDList;
import loci.formats.tiff.TiffParser;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ConversionTest {

  Path input;

  Path output;

  Path outputOmeTiff;

  Converter converter;

  PyramidFromDirectoryWriter writer;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  /**
   * Run the bioformats2raw main method and check for success or failure.
   *
   * @param additionalArgs CLI arguments as needed beyond "input output"
   */
  void assertBioFormats2Raw(String...additionalArgs) throws IOException {
    List<String> args = new ArrayList<String>();
    for (String arg : additionalArgs) {
      args.add(arg);
    }
    args.add(input.toString());
    output = tmp.newFolder().toPath().resolve("test");
    args.add(output.toString());
    try {
      converter = new Converter();
      CommandLine.call(converter, args.toArray(new String[]{}));
      Assert.assertTrue(Files.exists(output.resolve(".zattrs")));
      Assert.assertTrue(Files.exists(
        output.resolve("OME").resolve("METADATA.ome.xml")));
    }
    catch (RuntimeException rt) {
      throw rt;
    }
    catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  /**
   * Run the PyramidFromDirectoryWriter main method and check for success or
   * failure.
   *
   * @param additionalArgs CLI arguments as needed beyond "input output"
   */
  void assertTool(String...additionalArgs) throws IOException {
    assertTool(0, 0, additionalArgs);
  }

  /**
   * Run the PyramidFromDirectoryWriter main method and check for success or
   * failure.
   *
   * @param seriesCount number of series to expect, if splitting
   * @param fileCount number of files to expect, if splitting
   * @param additionalArgs CLI arguments as needed beyond "input output"
   */
  void assertTool(int seriesCount, int fileCount, String...additionalArgs)
    throws IOException
  {
    List<String> args = new ArrayList<String>();
    for (String arg : additionalArgs) {
      args.add(arg);
    }
    args.add(output.toString());
    outputOmeTiff = output.resolve("output.ome.tiff");
    args.add(outputOmeTiff.toString());
    try {
      writer = new PyramidFromDirectoryWriter();
      CommandLine.call(writer, args.toArray(new String[]{}));
      if (fileCount == 0) {
        Assert.assertTrue(Files.exists(outputOmeTiff));
      }
      else if (seriesCount == fileCount) {
        String prefix = output.resolve("output").toString();
        for (int i=0; i<fileCount; i++) {
          Assert.assertTrue(Files.exists(
            Paths.get(prefix + "_s" + i + ".ome.tiff")));
        }
        Assert.assertFalse(Files.exists(
            Paths.get(prefix + "_s" +
            fileCount + ".ome.tiff")));
      }
      else {
        String prefix = output.resolve("output").toString();
        for (int i=0; i<seriesCount; i++) {
          Assert.assertTrue(Files.exists(
            Paths.get(prefix + "_s" + i + "_z0_c0_t0.ome.tiff")));
        }
        Assert.assertFalse(Files.exists(Paths.get(prefix + "_s0.ome.tiff")));
      }
    }
    catch (RuntimeException rt) {
      throw rt;
    }
    catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  static Path fake(String...args) {
    Assert.assertTrue(args.length %2 == 0);
    Map<String, String> options = new HashMap<String, String>();
    for (int i = 0; i < args.length; i += 2) {
      options.put(args[i], args[i+1]);
    }
    return fake(options);
  }

  static Path fake(Map<String, String> options) {
    return fake(options, null);
  }

  /**
   * Create a Bio-Formats fake INI file to use for testing.
   * @param options map of the options to assign as part of the fake filename
   * from the allowed keys
   * @param series map of the integer series index and options map (same format
   * as <code>options</code> to add to the fake INI content
   * @see https://docs.openmicroscopy.org/bio-formats/6.4.0/developers/
   * generating-test-images.html#key-value-pairs
   * @return path to the fake INI file that has been created
   */
  static Path fake(Map<String, String> options,
          Map<Integer, Map<String, String>> series)
  {
    return fake(options, series, null);
  }

  static Path fake(Map<String, String> options,
          Map<Integer, Map<String, String>> series,
          Map<String, String> originalMetadata)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("image");
    if (options != null) {
      for (Map.Entry<String, String> kv : options.entrySet()) {
        sb.append("&");
        sb.append(kv.getKey());
        sb.append("=");
        sb.append(kv.getValue());
      }
    }
    sb.append("&");
    try {
      List<String> lines = new ArrayList<String>();
      if (originalMetadata != null) {
        lines.add("[GlobalMetadata]");
        for (String key : originalMetadata.keySet()) {
          lines.add(String.format("%s=%s", key, originalMetadata.get(key)));
        }
      }
      if (series != null) {
        for (int s : series.keySet()) {
          Map<String, String> seriesOptions = series.get(s);
          lines.add(String.format("[series_%d]", s));
          for (String key : seriesOptions.keySet()) {
            lines.add(String.format("%s=%s", key, seriesOptions.get(key)));
          }
        }
      }
      Path ini = Files.createTempFile(sb.toString(), ".fake.ini");
      File iniAsFile = ini.toFile();
      String iniPath = iniAsFile.getAbsolutePath();
      String fakePath = iniPath.substring(0, iniPath.length() - 4);
      Path fake = Paths.get(fakePath);
      File fakeAsFile = fake.toFile();
      Files.write(fake, new byte[]{});
      Files.write(ini, lines);
      iniAsFile.deleteOnExit();
      fakeAsFile.deleteOnExit();
      return ini;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Compare each pixel in each plane in each series of the input fake
   * and output OME-TIFF files.  Sub-resolutions are not checked.
   */
  void iteratePixels() throws Exception {
    try (ImageReader outputReader = new ImageReader()) {
      outputReader.setFlattenedResolutions(false);
      outputReader.setId(outputOmeTiff.toString());
      iteratePixels(outputReader);
    }
  }


  /**
   * Compare each pixel in each plane in each series of the input fake
   * and output OME-TIFF files.  Sub-resolutions are not checked.
   *
   * @param outputReader initialized reader for converted OME-TIFF
   */
  void iteratePixels(ImageReader outputReader) throws Exception {
    try (ImageReader inputReader = new ImageReader()) {
      inputReader.setId(input.toString());

      for (int series=0; series<inputReader.getSeriesCount(); series++) {
        inputReader.setSeries(series);
        outputReader.setSeries(series);

        Assert.assertEquals(
          inputReader.getImageCount(), outputReader.getImageCount());
        Assert.assertEquals(inputReader.getSizeC(), outputReader.getSizeC());
        for (int plane=0; plane<inputReader.getImageCount(); plane++) {
          Object inputPlane = getPlane(inputReader, plane);
          Object outputPlane = getPlane(outputReader, plane);

          int inputLength = Array.getLength(inputPlane);
          int outputLength = Array.getLength(outputPlane);
          Assert.assertEquals(inputLength, outputLength);
          for (int px=0; px<inputLength; px++) {
            Assert.assertEquals(
                Array.get(inputPlane, px), Array.get(outputPlane, px));
          }
        }
      }
    }
  }

  /**
   * Get the specified plane as a primitive array.
   *
   * @param reader initialized reader with correct series set
   * @param planeIndex plane to read
   * @return primitive array of pixels
   */
  Object getPlane(ImageReader reader, int planeIndex) throws Exception {
    byte[] rawPlane = reader.openBytes(planeIndex);
    int pixelType = reader.getPixelType();
    return DataTools.makeDataArray(rawPlane,
        FormatTools.getBytesPerPixel(pixelType),
        FormatTools.isFloatingPoint(pixelType),
        reader.isLittleEndian());
  }

  private void assertDefaults() throws Exception {
    ZarrArray series0 = ZarrGroup.open(output.resolve("0")).openArray("0");
    // no getter for DimensionSeparator in ZarrArray
    // check that the correct separator was used by checking
    // that the expected first chunk file exists
    Assert.assertTrue(output.resolve("0/0/0/0/0/0/0").toFile().exists());
    // Also ensure we're using the latest .zarray metadata
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode root = objectMapper.readTree(
        output.resolve("0/0/.zarray").toFile());
    Assert.assertEquals("/", root.path("dimension_separator").asText());
    try (ImageReader reader = new ImageReader()) {
      reader.setFlattenedResolutions(false);
      reader.setId(outputOmeTiff.toString());
      Assert.assertEquals(2, reader.getResolutionCount());
    }
    iteratePixels();
  }

  /**
   * Test defaults.
   */
  @Test
  public void testDefaults() throws Exception {
    input = fake();
    assertBioFormats2Raw();
    assertTool();
    assertDefaults();
  }

  /**
   * Test symlink as the root.
   */
  @Test
  public void testSymlinkAsRoot() throws Exception {
    Assume.assumeTrue(SystemUtils.IS_OS_LINUX);
    input = fake();
    assertBioFormats2Raw();
    Path notASymlink = output.resolveSibling(output.getFileName() + ".old");
    Files.move(output, notASymlink);
    Files.createSymbolicLink(output, notASymlink);
    assertTool();
    assertDefaults();
  }

  /**
   * Test series count check.
   */
  @Test
  public void testSeriesCountCheck() throws Exception {
    input = fake();
    assertBioFormats2Raw();
    Files.delete(output.resolve("0").resolve(".zgroup"));
    try {
      assertTool();
    }
    catch (ExecutionException e) {
      // First cause is RuntimeException wrapping the checked FormatException
      Assert.assertEquals(
          FormatException.class, e.getCause().getCause().getClass());
      return;
    }
    Assert.fail("Did not throw exception on invalid data");
  }

  /**
   * Test South and East edge padding.
   */
  @Test
  public void testSouthEastEdgePadding() throws Exception {
    input = fake();
    assertBioFormats2Raw("-w", "240", "-h", "240");
    assertTool("--compression", "raw");
    try (ImageReader reader = new ImageReader()) {
      reader.setFlattenedResolutions(false);
      reader.setId(outputOmeTiff.toString());
      Assert.assertEquals(2, reader.getResolutionCount());
      Assert.assertEquals(240, reader.getOptimalTileWidth());
      Assert.assertEquals(240, reader.getOptimalTileHeight());
      ByteBuffer plane = ByteBuffer.wrap(reader.openBytes(0));
      Assert.assertEquals(512 * 512, plane.capacity());
      int offset = 0;
      for (int y = 0; y < reader.getSizeY(); y++) {
        offset = (y * 512) + 511;
        Assert.assertEquals(255, Byte.toUnsignedInt(plane.get(offset)));
      }
    }
    try (TiffParser tiffParser = new TiffParser(outputOmeTiff.toString())) {
      IFDList mainIFDs = tiffParser.getMainIFDs();
      Assert.assertEquals(1, mainIFDs.size());
      int tileSize = 240 * 240;
      Assert.assertArrayEquals(new long[] {
        tileSize, tileSize, tileSize,  // Row 1
        tileSize, tileSize, tileSize,  // Row 2
        tileSize, tileSize, tileSize   // Row 3
      }, mainIFDs.get(0).getStripByteCounts());
    }
    iteratePixels();
  }

  /**
   * Test edge padding uint16.
   */
  @Test
  public void testEdgePaddingUint16() throws Exception {
    input = fake("pixelType", "uint16");
    assertBioFormats2Raw("-w", "240", "-h", "240");
    assertTool("--compression", "raw");
    try (ImageReader reader = new ImageReader()) {
      reader.setFlattenedResolutions(false);
      reader.setId(outputOmeTiff.toString());
      Assert.assertEquals(2, reader.getResolutionCount());
      Assert.assertEquals(240, reader.getOptimalTileWidth());
      Assert.assertEquals(240, reader.getOptimalTileHeight());
      ShortBuffer plane = ByteBuffer.wrap(reader.openBytes(0)).asShortBuffer();
      Assert.assertEquals(512 * 512, plane.capacity());
      int offset = 0;
      for (int y = 0; y < reader.getSizeY(); y++) {
        offset = (y * 512) + 511;
        Assert.assertEquals(511, Short.toUnsignedInt(plane.get(offset)));
      }
    }
    try (TiffParser tiffParser = new TiffParser(outputOmeTiff.toString())) {
      IFDList mainIFDs = tiffParser.getMainIFDs();
      Assert.assertEquals(1, mainIFDs.size());
      int tileSize = 240 * 240 * 2;
      Assert.assertArrayEquals(new long[] {
        tileSize, tileSize, tileSize,  // Row 1
        tileSize, tileSize, tileSize,  // Row 2
        tileSize, tileSize, tileSize   // Row 3
      }, mainIFDs.get(0).getStripByteCounts());
    }
    iteratePixels();
  }

  /**
   * Test 17x19 tile size with uint16 data.
   */
  @Test
  public void testOddTileSize() throws Exception {
    input = fake("pixelType", "uint16");
    assertBioFormats2Raw("-w", "17", "-h", "19");
    assertTool("--compression", "raw");
    iteratePixels();

    try (TiffParser parser = new TiffParser(outputOmeTiff.toString())) {
      IFDList mainIFDs = parser.getMainIFDs();
      Assert.assertEquals(1, mainIFDs.size());
      int tileSize = 32 * 32 * 2;
      long[] tileByteCounts = mainIFDs.get(0).getStripByteCounts();
      Assert.assertEquals(256, tileByteCounts.length);
      for (long count : tileByteCounts) {
        Assert.assertEquals(tileSize, count);
      }
    }
  }

  /**
   * Test 128x128 tile size with 497x498 image.
   */
  @Test
  public void testOddImageSize() throws Exception {
    input = fake("sizeX", "497", "sizeY", "498", "pixelType", "uint16");
    assertBioFormats2Raw("-w", "128", "-h", "128");
    assertTool();
    iteratePixels();
  }

  /**
   * Test RGB with multiple timepoints.
   */
  @Test
  public void testRGBMultiT() throws Exception {
    input = fake("sizeC", "3", "sizeT", "5", "rgb", "3");
    assertBioFormats2Raw();
    assertTool("--rgb");
    iteratePixels();
    try (ImageReader reader = new ImageReader()) {
      ServiceFactory sf = new ServiceFactory();
      OMEXMLService xmlService = sf.getInstance(OMEXMLService.class);
      OMEXMLMetadata metadata = xmlService.createOMEXMLMetadata();
      reader.setMetadataStore(metadata);
      reader.setFlattenedResolutions(false);
      reader.setId(outputOmeTiff.toString());
      Assert.assertEquals(
          3, metadata.getPixelsSizeC(0).getNumberValue());
      Assert.assertEquals(1, metadata.getChannelCount(0));
      Assert.assertEquals(
          3, metadata.getChannelSamplesPerPixel(0, 0).getNumberValue());
      Assert.assertNull(metadata.getChannelColor(0, 0));
      Assert.assertNull(metadata.getChannelEmissionWavelength(0, 0));
      Assert.assertNull(metadata.getChannelExcitationWavelength(0, 0));
      Assert.assertNull(metadata.getChannelName(0, 0));
    }
    checkRGBIFDs();
  }

  /**
   * Test RGB with multiple channels.
   */
  @Test
  public void testRGBMultiC() throws Exception {
    input = fake("sizeC", "12", "rgb", "3");
    assertBioFormats2Raw();
    assertTool("--rgb");
    iteratePixels();
    try (ImageReader reader = new ImageReader()) {
      ServiceFactory sf = new ServiceFactory();
      OMEXMLService xmlService = sf.getInstance(OMEXMLService.class);
      OMEXMLMetadata metadata = xmlService.createOMEXMLMetadata();
      reader.setMetadataStore(metadata);
      reader.setFlattenedResolutions(false);
      reader.setId(outputOmeTiff.toString());
      Assert.assertEquals(
          12, metadata.getPixelsSizeC(0).getNumberValue());
      Assert.assertEquals(4, metadata.getChannelCount(0));
      Assert.assertEquals(
          3, metadata.getChannelSamplesPerPixel(0, 0).getNumberValue());
      Assert.assertNull(metadata.getChannelColor(0, 0));
      Assert.assertNull(metadata.getChannelEmissionWavelength(0, 0));
      Assert.assertNull(metadata.getChannelExcitationWavelength(0, 0));
      Assert.assertNull(metadata.getChannelName(0, 0));
      Assert.assertEquals(
        3, metadata.getChannelSamplesPerPixel(0, 1).getNumberValue());
      Assert.assertNull(metadata.getChannelColor(0, 1));
      Assert.assertNull(metadata.getChannelEmissionWavelength(0, 1));
      Assert.assertNull(metadata.getChannelExcitationWavelength(0, 1));
      Assert.assertNull(metadata.getChannelName(0, 1));
      Assert.assertEquals(
        3, metadata.getChannelSamplesPerPixel(0, 2).getNumberValue());
      Assert.assertNull(metadata.getChannelColor(0, 2));
      Assert.assertNull(metadata.getChannelEmissionWavelength(0, 2));
      Assert.assertNull(metadata.getChannelExcitationWavelength(0, 2));
      Assert.assertNull(metadata.getChannelName(0, 2));
      Assert.assertEquals(
        3, metadata.getChannelSamplesPerPixel(0, 3).getNumberValue());
      Assert.assertNull(metadata.getChannelColor(0, 3));
      Assert.assertNull(metadata.getChannelEmissionWavelength(0, 3));
      Assert.assertNull(metadata.getChannelExcitationWavelength(0, 3));
      Assert.assertNull(metadata.getChannelName(0, 3));
    }
    checkRGBIFDs();
  }

  /**
   * Test RGB with channel metadata.
   */
  @Test
  public void testRGBChannelMetadata() throws Exception {
    Map<String, String> options = new HashMap<String, String>();
    options.put("sizeC", "3");
    options.put("rgb", "3");
    options.put("color_0", "16711935");
    Map<Integer, Map<String, String>> series =
        new HashMap<Integer, Map<String, String>>();
    Map<String, String> series0 = new HashMap<String, String>();
    series0.put("ChannelName_0", "FITC");
    series.put(0, series0);
    input = fake(options, series);
    assertBioFormats2Raw();
    assertTool("--rgb");
    iteratePixels();
    try (ImageReader reader = new ImageReader()) {
      ServiceFactory sf = new ServiceFactory();
      OMEXMLService xmlService = sf.getInstance(OMEXMLService.class);
      OMEXMLMetadata metadata = xmlService.createOMEXMLMetadata();
      reader.setMetadataStore(metadata);
      reader.setFlattenedResolutions(false);
      reader.setId(outputOmeTiff.toString());
      Assert.assertEquals(
          3, metadata.getPixelsSizeC(0).getNumberValue());
      Assert.assertEquals(1, metadata.getChannelCount(0));
      Assert.assertEquals(
          3, metadata.getChannelSamplesPerPixel(0, 0).getNumberValue());
      Assert.assertNull(metadata.getChannelColor(0, 0));
      Assert.assertNull(metadata.getChannelEmissionWavelength(0, 0));
      Assert.assertNull(metadata.getChannelExcitationWavelength(0, 0));
      Assert.assertNull(metadata.getChannelName(0, 0));
    }
    checkRGBIFDs();
  }

  /**
   * Test TIFF metadata.
   */
  @Test
  public void testMetadata() throws Exception {
    input = fake("physicalSizeX", "0.5", "physicalSizeY", "0.6");
    assertBioFormats2Raw();
    assertTool();

    try (TiffParser parser = new TiffParser(outputOmeTiff.toString())) {
      IFDList mainIFDs = parser.getMainIFDs();
      Assert.assertEquals(1, mainIFDs.size());
      IFD ifd = mainIFDs.get(0);
      Assert.assertEquals(ifd.getXResolution(), 0.5, 0.0001);
      Assert.assertEquals(ifd.getYResolution(), 0.6, 0.0001);
    }
  }

  /**
   * Test small plate.
   */
  @Test
  public void testPlate() throws Exception {
    input =
      fake("plateRows", "2", "plateCols", "3", "fields", "4", "sizeC", "3");
    assertBioFormats2Raw();
    assertTool();
    iteratePixels();
    try (ImageReader reader = new ImageReader()) {
      ServiceFactory sf = new ServiceFactory();
      OMEXMLService xmlService = sf.getInstance(OMEXMLService.class);
      OMEXMLMetadata metadata = xmlService.createOMEXMLMetadata();
      reader.setMetadataStore(metadata);
      reader.setFlattenedResolutions(false);
      reader.setId(outputOmeTiff.toString());
      Assert.assertEquals(24, reader.getSeriesCount());
      Assert.assertEquals(24, metadata.getImageCount());
      Assert.assertEquals(1, metadata.getPlateCount());
    }
  }

  /**
   * Test single image no HCS.
   */
  @Test
  public void testSingleImageNoHCS() throws Exception {
    input =
      fake("plateRows", "2", "plateCols", "3", "fields", "4", "sizeC", "3");
    assertBioFormats2Raw("--series", "0", "--no-hcs");
    assertTool();
    try (ImageReader reader = new ImageReader()) {
      ServiceFactory sf = new ServiceFactory();
      OMEXMLService xmlService = sf.getInstance(OMEXMLService.class);
      OMEXMLMetadata metadata = xmlService.createOMEXMLMetadata();
      reader.setMetadataStore(metadata);
      reader.setFlattenedResolutions(false);
      reader.setId(outputOmeTiff.toString());
      Assert.assertEquals(1, reader.getSeriesCount());
      Assert.assertEquals(1, metadata.getImageCount());
      Assert.assertEquals(0, metadata.getPlateCount());
    }
  }

  /**
   * Test splitting series into separate files.
   */
  @Test
  public void testSplitFiles() throws Exception {
    input =
      fake("plateRows", "2", "plateCols", "3", "fields", "4", "sizeC", "3");
    assertBioFormats2Raw();
    assertTool(24, 24, "--split");

    try (ImageReader reader = new ImageReader()) {
      ServiceFactory sf = new ServiceFactory();
      OMEXMLService xmlService = sf.getInstance(OMEXMLService.class);
      OMEXMLMetadata metadata = xmlService.createOMEXMLMetadata();
      reader.setMetadataStore(metadata);
      reader.setFlattenedResolutions(false);

      // --split should always produce a companion OME-XML file
      // with BinaryOnly OME-TIFFs
      reader.setId(output.resolve("output").toString() + ".companion.ome");

      Assert.assertEquals(reader.getUsedFiles().length, 25);
      Assert.assertEquals(reader.getSeriesCount(), 24);
      Assert.assertEquals(1, metadata.getPlateCount());
      Assert.assertEquals(24, metadata.getImageCount());

      iteratePixels(reader);
    }
  }

  /**
   * Test splitting planes into separate files.
   */
  @Test
  public void testSplitPlanes() throws Exception {
    input =
      fake("plateRows", "2", "plateCols", "3", "fields", "4", "sizeC", "3");
    assertBioFormats2Raw();
    assertTool(24, 72, "--split-planes");

    try (ImageReader reader = new ImageReader()) {
      ServiceFactory sf = new ServiceFactory();
      OMEXMLService xmlService = sf.getInstance(OMEXMLService.class);
      OMEXMLMetadata metadata = xmlService.createOMEXMLMetadata();
      reader.setMetadataStore(metadata);
      reader.setFlattenedResolutions(false);

      // --split-planes should always produce a companion OME-XML file
      // with BinaryOnly OME-TIFFs
      reader.setId(output.resolve("output").toString() + ".companion.ome");

      Assert.assertEquals(reader.getUsedFiles().length, 73);
      Assert.assertEquals(reader.getSeriesCount(), 24);
      Assert.assertEquals(1, metadata.getPlateCount());
      Assert.assertEquals(24, metadata.getImageCount());

      iteratePixels(reader);
    }
  }

  /**
   * Test what happens when the split-by-series and split-by-plane
   * options are used together.
   */
  @Test
  public void testBothSplitOptions() throws Exception {
    input =
      fake("plateRows", "2", "plateCols", "3", "fields", "4", "sizeC", "3");
    assertBioFormats2Raw();
    // expect --split-planes to take precedence
    assertTool(24, 72, "--split", "--split-planes");

    try (ImageReader reader = new ImageReader()) {
      ServiceFactory sf = new ServiceFactory();
      OMEXMLService xmlService = sf.getInstance(OMEXMLService.class);
      OMEXMLMetadata metadata = xmlService.createOMEXMLMetadata();
      reader.setMetadataStore(metadata);
      reader.setFlattenedResolutions(false);

      // --split-planes should always produce a companion OME-XML file
      // with BinaryOnly OME-TIFFs
      reader.setId(output.resolve("output").toString() + ".companion.ome");

      Assert.assertEquals(reader.getUsedFiles().length, 73);
      Assert.assertEquals(reader.getSeriesCount(), 24);
      Assert.assertEquals(1, metadata.getPlateCount());
      Assert.assertEquals(24, metadata.getImageCount());

      iteratePixels(reader);
    }
  }

  /**
   * Test RGB with multiple channels using API instead of command line.
   */
  @Test
  public void testOptionsAPI() throws Exception {
    input = fake("sizeC", "12", "rgb", "3");
    assertBioFormats2Raw();

    outputOmeTiff = output.resolve("output.ome.tiff");
    PyramidFromDirectoryWriter apiConverter = new PyramidFromDirectoryWriter();
    CommandLine cmd = new CommandLine(apiConverter);
    cmd.parseArgs();

    apiConverter.setInputPath(output.toString());
    apiConverter.setOutputPath(outputOmeTiff.toString());
    apiConverter.setCompression(CompressionType.UNCOMPRESSED);
    apiConverter.setRGB(true);
    apiConverter.call();

    iteratePixels();
    checkRGBIFDs();
  }

  /**
   * Test resetting options to their default values.
   */
  @Test
  public void testResetAPI() throws Exception {
    input = fake("sizeC", "12", "rgb", "3");
    assertBioFormats2Raw();

    outputOmeTiff = output.resolve("output.ome.tiff");

    // make sure default options are set
    PyramidFromDirectoryWriter apiConverter = new PyramidFromDirectoryWriter();
    CommandLine cmd = new CommandLine(apiConverter);
    cmd.parseArgs();

    Assert.assertEquals(apiConverter.getInputPath(), null);
    Assert.assertEquals(apiConverter.getOutputPath(), null);
    Assert.assertEquals(apiConverter.getCompression(), CompressionType.LZW);
    Assert.assertEquals(apiConverter.getRGB(), false);
    Assert.assertEquals(apiConverter.getLegacyTIFF(), false);
    Assert.assertEquals(apiConverter.getSplitTIFFs(), false);

    // override default options, as though to start a conversion
    apiConverter.setInputPath(output.toString());
    apiConverter.setOutputPath(outputOmeTiff.toString());
    apiConverter.setCompression(CompressionType.UNCOMPRESSED);
    apiConverter.setRGB(true);

    // change our minds and reset the options to defaults again
    cmd.parseArgs();

    Assert.assertEquals(apiConverter.getInputPath(), null);
    Assert.assertEquals(apiConverter.getOutputPath(), null);
    Assert.assertEquals(apiConverter.getCompression(), CompressionType.LZW);
    Assert.assertEquals(apiConverter.getRGB(), false);
    Assert.assertEquals(apiConverter.getLegacyTIFF(), false);
    Assert.assertEquals(apiConverter.getSplitTIFFs(), false);

    // update options, make sure they were set, and actually convert
    apiConverter.setInputPath(output.toString());
    apiConverter.setOutputPath(outputOmeTiff.toString());
    apiConverter.setCompression(CompressionType.UNCOMPRESSED);
    apiConverter.setRGB(true);

    Assert.assertEquals(apiConverter.getInputPath(), output.toString());
    Assert.assertEquals(apiConverter.getOutputPath(), outputOmeTiff.toString());
    Assert.assertEquals(apiConverter.getCompression(),
      CompressionType.UNCOMPRESSED);
    Assert.assertEquals(apiConverter.getRGB(), true);

    apiConverter.call();

    iteratePixels();
  }

  /**
   * Test "--quality" command line option.
   */
  @Test
  public void testCompressionQuality() throws Exception {
    input = fake("sizeC", "3", "rgb", "3");
    assertBioFormats2Raw();
    assertTool("--compression", "JPEG-2000", "--quality", "0.25", "--rgb");

    try (ImageReader reader = new ImageReader()) {
      reader.setFlattenedResolutions(false);
      reader.setId(outputOmeTiff.toString());
      Assert.assertEquals(1, reader.getSeriesCount());
      Assert.assertEquals(3, reader.getRGBChannelCount());
    }
    checkRGBIFDs();
  }

  /**
   * Test "--version" with no other arguments.
   * Does not test the version values, just makes sure that an exception
   * is not thrown.
   */
  @Test
  public void testVersionOnly() throws Exception {
    CommandLine.call(
      new PyramidFromDirectoryWriter(), new String[] {"--version"});
  }

  /**
   * Test conversion of a single multiscales, similar to a label image.
   */
  @Test
  public void testLabelImage() throws Exception {
    input = fake("sizeX", "2000", "sizeY", "1500");
    assertBioFormats2Raw();
    output = output.resolve("0");
    assertTool("-f", input.toString());
    iteratePixels();
  }

  /**
   * Test conversion of a single multiscales (similar to label image),
   * but intentionally provide incorrect image metadata.
   */
  @Test
  public void testLabelImageWrongSize() throws Exception {
    input = fake("sizeX", "2000", "sizeY", "1500");
    assertBioFormats2Raw();
    output = output.resolve("0");
    try {
      assertTool("-f", "test.fake");
    }
    catch (ExecutionException e) {
      // First cause is RuntimeException wrapping the checked FormatException
      Assert.assertEquals(
          FormatException.class, e.getCause().getCause().getClass());
      return;
    }
    Assert.fail("Did not throw exception on invalid data");
  }

  /**
   * Test conversion of single multiscales (label image), where the provided
   * metadata has more channels than the Zarr.
   */
  @Test
  public void testLabelImageExtraMetadataChannels() throws Exception {
    input = fake("sizeX", "2000", "sizeY", "1500");
    assertBioFormats2Raw();
    output = output.resolve("0");

    Path metadata = fake("sizeX", "2000", "sizeY", "1500", "sizeC", "3");
    assertTool("-f", metadata.toString());
    iteratePixels();
  }

  /**
   * Test conversion of single multiscales (label image), where the provided
   * metadata has fewer channels than the Zarr.
   * Extra channels should be inserted into the metadata.
   */
  @Test
  public void testLabelImageNotEnoughMetadataChannels() throws Exception {
    input = fake("sizeX", "2000", "sizeY", "1500", "sizeC", "3");
    assertBioFormats2Raw();
    output = output.resolve("0");

    Path metadata = fake("sizeX", "2000", "sizeY", "1500", "sizeC", "1");
    assertTool("-f", metadata.toString());
    iteratePixels();
  }

  /**
   * Test conversion of single multiscales (label image), where the provided
   * metadata has different pixel type and endianness compared to the Zarr.
   * Pixel type and endianness should be inherited from the Zarr,
   * otherwise the output data is likely to be incorrect.
   */
  @Test
  public void testLabelImagePixelTypeMismatch() throws Exception {
    input = fake("sizeX", "2000", "sizeY", "1500",
      "pixelType", "uint16", "little", "true");
    assertBioFormats2Raw();
    output = output.resolve("0");

    Path metadata = fake("sizeX", "2000", "sizeY", "1500",
      "pixelType", "uint8", "little", "false");
    assertTool("-f", metadata.toString());
    iteratePixels();
  }

  private void checkRGBIFDs() throws FormatException, IOException {
    try (TiffParser parser = new TiffParser(outputOmeTiff.toString())) {
      IFDList mainIFDs = parser.getMainIFDs();
      for (IFD ifd : mainIFDs) {
        Assert.assertEquals(1, ifd.getPlanarConfiguration());
        Assert.assertEquals(3, ifd.getSamplesPerPixel());

        IFDList subresolutions = parser.getSubIFDs(ifd);
        for (IFD subres : subresolutions) {
          Assert.assertEquals(1, subres.getPlanarConfiguration());
          Assert.assertEquals(3, ifd.getSamplesPerPixel());
        }
      }
    }
  }

}
