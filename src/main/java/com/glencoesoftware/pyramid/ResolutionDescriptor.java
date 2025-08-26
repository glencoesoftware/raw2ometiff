/**
 * Copyright (c) 2019-2020 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.pyramid;

import java.util.ArrayList;

public class ResolutionDescriptor {
  /** Path to resolution. */
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

  /** Axes in the underlying array, in order. */
  ArrayList<String> axes = new ArrayList<String>();

  /**
   * Add named axis to ordered list of axes in this resolution.
   * Names are stored as upper-case only.
   *
   * @param axis name e.g. "x"
   */
  public void addAxis(String axis) {
    axes.add(axis.toUpperCase());
  }

  /**
   * Find the index in the ordered list of the named axis.
   *
   * @param axis name e.g. "x"
   * @return index into list of axes
   */
  public int getIndex(String axis) {
    return axes.indexOf(axis.toUpperCase());
  }

  /**
   * Create an indexing array (e.g. shape or offset) for this resolution,
   * which represents the given 5D values.
   * Since the resolution's underlying array may have less than 5 dimensions,
   * this is mapping from the 5D space of the OME data model to the
   * ND space of this resolution's array.
   *
   * @param t T index
   * @param c C index
   * @param z Z index
   * @param y Y index
   * @param x X index
   * @return array representing the given indexes, in this resolution's
   * dimensional space
   */
  public int[] getArray(int t, int c, int z, int y, int x) {
    int[] returnArray = new int[axes.size()];
    for (int i=0; i<axes.size(); i++) {
      char axis = axes.get(i).charAt(0);
      switch (axis) {
        case 'X':
          returnArray[i] = x;
          break;
        case 'Y':
          returnArray[i] = y;
          break;
        case 'Z':
          returnArray[i] = z;
          break;
        case 'C':
          returnArray[i] = c;
          break;
        case 'T':
          returnArray[i] = t;
          break;
        default:
          throw new IllegalArgumentException("Unexpected axis: " + axis);
      }
    }
    return returnArray;
  }

}
