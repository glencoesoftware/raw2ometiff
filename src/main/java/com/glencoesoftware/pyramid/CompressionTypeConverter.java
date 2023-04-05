/**
 * Copyright (c) 2023 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */

package com.glencoesoftware.pyramid;

import picocli.CommandLine.ITypeConverter;

/**
 * Convert a string to a CompressionType.
 */
public class CompressionTypeConverter
  implements ITypeConverter<CompressionType>
{
  @Override
  public CompressionType convert(String value) throws Exception {
    return CompressionType.lookup(value);
  }
}
