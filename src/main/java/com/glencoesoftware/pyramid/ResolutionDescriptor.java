/**
 * Copyright (c) 2019-2020 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.pyramid;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import loci.formats.FormatTools;
import loci.formats.Modulo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResolutionDescriptor {

  private static final Logger LOG =
    LoggerFactory.getLogger(ResolutionDescriptor.class);

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
  ArrayList<Integer> axisLengths = new ArrayList<Integer>();

  Modulo moduloZ;
  Modulo moduloC;
  Modulo moduloT;

  /**
   * Add named axis to ordered list of axes in this resolution.
   * Names are stored as upper-case only.
   *
   * @param axis name e.g. "x"
   * @param len axis length
   */
  public void addAxis(String axis, int len) {
    axes.add(axis.toUpperCase());
    axisLengths.add(len);
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
   * @param no plane index
   * @param yi Y index
   * @param xi X index
   * @return array representing the given indexes, in this resolution's
   * dimensional space
   */
  public int[] getArray(int no, int yi, int xi) {
    int[] returnArray = new int[axes.size()];
    int[] lengths = new int[axisLengths.size() - 2];
    int xIndex = getIndex("X");
    int yIndex = getIndex("Y");
    int index = lengths.length - 1;
    for (int i=0; i<axisLengths.size(); i++) {
      if (i == xIndex || i == yIndex) {
        continue;
      }
      lengths[index] = axisLengths.get(i);
      index--;
    }
    // this should be in roughly ZCT order
    int[] pos = FormatTools.rasterToPosition(lengths, no);
    int nextPos = pos.length - 1;
    for (int i=0; i<axes.size(); i++) {
      char axis = axes.get(i).charAt(0);
      switch (axis) {
        case 'X':
          returnArray[i] = xi;
          break;
        case 'Y':
          returnArray[i] = yi;
          break;
        default:
          returnArray[i] = pos[nextPos];
          nextPos--;
      }
    }
    return returnArray;
  }

  /**
   * Get an appropriately-sized shape array for the given XY.
   * All axes other than X and Y will be 1.
   *
   * @param yi Y shape
   * @param xi X shape
   *
   * @return shape array
   */
  public int[] getShapeArray(int yi, int xi) {
    int[] returnArray = new int[axes.size()];
    for (int i=0; i<axes.size(); i++) {
      char axis = axes.get(i).charAt(0);
      switch (axis) {
        case 'X':
          returnArray[i] = xi;
          break;
        case 'Y':
          returnArray[i] = yi;
          break;
        default:
          returnArray[i] = 1;
      }
    }
    return returnArray;
  }

  protected void parseMultiscales(
    List<Map<String, Object>> multiscales, int[] shape)
  {
    Map<String, Object> multiscale = multiscales.get(0);
    List<Map<String, Object>> storedAxes = null;
    if (multiscales != null) {
      storedAxes = (List<Map<String, Object>>) multiscale.get("axes");
    }

    if (storedAxes != null) {
      for (int i=0; i<shape.length; i++) {
        addAxis(storedAxes.get(i).get("name").toString(), shape[i]);
      }
    }
    else if (shape.length == 5) {
      addAxis("T", shape[4]);
      addAxis("C", shape[3]);
      addAxis("Z", shape[2]);
      addAxis("Y", shape[1]);
      addAxis("X", shape[0]);
    }
    else {
      LOG.error("No stored 'axes' and array shape length {}", shape.length);
    }
  }

}
