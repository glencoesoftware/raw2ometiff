/**
 * Copyright (c) 2019-2020 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.pyramid;

import java.util.EnumSet;
import loci.formats.out.TiffWriter;
import loci.formats.tiff.TiffCompression;

/**
 * List of valid compression types for the output OME-TIFF file.
 */
public enum CompressionType {
  UNCOMPRESSED(
    TiffWriter.COMPRESSION_UNCOMPRESSED, TiffCompression.UNCOMPRESSED),
  LZW(TiffWriter.COMPRESSION_LZW, TiffCompression.LZW),
  JPEG(TiffWriter.COMPRESSION_JPEG, TiffCompression.JPEG),
  JPEG_2000(TiffWriter.COMPRESSION_J2K, TiffCompression.JPEG_2000),
  JPEG_2000_LOSSY(
    TiffWriter.COMPRESSION_J2K_LOSSY, TiffCompression.JPEG_2000_LOSSY);

  private String compressionName;
  private TiffCompression compressionType;

  /**
   * Construct a list of valid compression types.
   *
   * @param name compression name (used in command line arguments)
   * @param type corresponding TIFF compression
   */
  private CompressionType(String name, TiffCompression type) {
    compressionName = name;
    compressionType = type;
  }

  /**
   * Find the compression corresponding to the given name.
   * If there is no matching name, return the uncompressed type.
   *
   * @param compressionName desired compression name
   * @return corresponding CompressionType, or UNCOMPRESSED if no match
   */
  public static CompressionType lookup(String compressionName) {
    for (CompressionType t : EnumSet.allOf(CompressionType.class)) {
      if (t.getName().equals(compressionName)) {
        return t;
      }
    }
    return UNCOMPRESSED;
  }

  /**
   * @return name of this compression type
   */
  public String getName() {
    return compressionName;
  }

  /**
   * @return TiffCompression for this compression type
   */
  public TiffCompression getTIFFCompression() {
    return compressionType;
  }

}
