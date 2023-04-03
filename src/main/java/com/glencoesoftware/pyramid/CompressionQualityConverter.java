/**
 * Copyright (c) 2023 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */

package com.glencoesoftware.pyramid;

import loci.formats.codec.CodecOptions;
import loci.formats.codec.JPEG2000CodecOptions;

import picocli.CommandLine.ITypeConverter;

/**
 * Convert a string to a CodecOptions.
 */
public class CompressionQualityConverter
  implements ITypeConverter<CodecOptions>
{
  @Override
  public CodecOptions convert(String value) throws Exception {
    // JPEG2000CodecOptions used here as it's the only way to pass
    // a quality value through to the JPEG-2000 codecs
    // this could be changed later if/when we support options on other codecs
    CodecOptions options = JPEG2000CodecOptions.getDefaultOptions();
    options.quality = Double.parseDouble(value);
    return options;
  }
}
