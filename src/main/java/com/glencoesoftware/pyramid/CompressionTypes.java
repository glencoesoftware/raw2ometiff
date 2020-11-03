/**
 * Copyright (c) 2019-2020 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.pyramid;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import loci.formats.out.TiffWriter;
import loci.formats.tiff.TiffCompression;

/**
 * List of valid compression types for the output OME-TIFF file.
 */
public class CompressionTypes extends ArrayList<String> {

  /**
   * Construct a list of valid compression types.
   */
  public CompressionTypes() {
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

  /**
   * Convert the compression argument to a TiffCompression object that can
   * be used to compress tiles.
   *
   * @param compression type
   * @return TiffCompression corresponding to the compression argument,
   */
  public static TiffCompression getTIFFCompression(String compression) {
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
}
