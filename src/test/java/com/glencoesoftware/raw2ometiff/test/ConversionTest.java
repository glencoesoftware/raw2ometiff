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
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.glencoesoftware.bioformats2raw.Converter;
import com.glencoesoftware.pyramid.PyramidFromDirectoryWriter;

import loci.formats.ImageReader;
import loci.formats.tiff.IFDList;
import loci.formats.tiff.TiffParser;
import picocli.CommandLine;

import org.junit.Assert;
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
      Assert.assertTrue(Files.exists(output.resolve("data.n5")));
      Assert.assertTrue(Files.exists(output.resolve("METADATA.ome.xml")));
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
      Assert.assertTrue(Files.exists(outputOmeTiff));
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
   * Test defaults.
   */
  @Test
  public void testDefaults() throws Exception {
    input = fake();
    assertBioFormats2Raw();
    assertTool();
    try (ImageReader reader = new ImageReader()) {
      reader.setFlattenedResolutions(false);
      reader.setId(outputOmeTiff.toString());
      Assert.assertEquals(2, reader.getResolutionCount());
    }
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
  }

}
