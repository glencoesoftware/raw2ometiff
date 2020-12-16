/**
 * Copyright (c) 2019-2020 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.pyramid;

public class ResolutionDescriptor {
  /** Path to resolution in N5/Zarr. */
  String path;

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
