/**
 * Copyright (c) 2019-2020 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.pyramid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.ome.OMEPyramidStore;
import loci.formats.tiff.IFDList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;

public class PyramidSeries {

  private static final Logger LOG =
    LoggerFactory.getLogger(PyramidSeries.class);

  /** Path to series. */
  String path;

  String uuid;

  int index = -1;

  IFDList[] ifds;

  /** FormatTools pixel type. */
  Integer pixelType;

  /** Number of resolutions. */
  int numberOfResolutions;

  boolean littleEndian = false;

  int planeCount = 1;
  int z = 1;
  int c = 1;
  int t = 1;
  String dimensionOrder;
  int[] dimensionLengths = new int[3];

  boolean rgb = false;

  /** Description of each resolution in the pyramid. */
  List<ResolutionDescriptor> resolutions;

  /**
   * Calculate image width and height for each resolution.
   * Uses the first tile in the resolution to find the tile size.
   *
   * @param reader reader used to get dataset attributes
   * @param metadata additional OME-XML metadata
   */
  public void describePyramid(ZarrGroup reader, OMEPyramidStore metadata)
    throws FormatException, IOException
  {
    LOG.info("Number of resolution levels: {}", numberOfResolutions);
    resolutions = new ArrayList<ResolutionDescriptor>();
    for (int resolution = 0; resolution < numberOfResolutions; resolution++) {
      ResolutionDescriptor descriptor = new ResolutionDescriptor();
      descriptor.resolutionNumber = resolution;
      descriptor.path = path + "/" + resolution;

      ZarrArray array = reader.openArray(descriptor.path);
      int[] dimensions = array.getShape();
      int[] blockSizes = array.getChunks();

      descriptor.sizeX = dimensions[dimensions.length - 1];
      descriptor.sizeY = dimensions[dimensions.length - 2];
      descriptor.tileSizeX = blockSizes[blockSizes.length - 1];
      descriptor.tileSizeY = blockSizes[blockSizes.length - 2];

      if (descriptor.tileSizeX % 16 != 0) {
        LOG.debug("Tile width ({}) not a multiple of 16; correcting",
          descriptor.tileSizeX);
        descriptor.tileSizeX += (16 - (descriptor.tileSizeX % 16));
      }
      if (descriptor.tileSizeY % 16 != 0) {
        LOG.debug("Tile height ({}) not a multiple of 16; correcting",
          descriptor.tileSizeY);
        descriptor.tileSizeY += (16 - (descriptor.tileSizeY % 16));
      }

      descriptor.numberOfTilesX =
        getTileCount(descriptor.sizeX, descriptor.tileSizeX);
      descriptor.numberOfTilesY =
        getTileCount(descriptor.sizeY, descriptor.tileSizeY);

      if (resolution == 0) {
        // If we have image metadata available sanity check the dimensions
        // against those in the underlying pyramid
        if (metadata.getImageCount() > 0) {
          int sizeX =
            metadata.getPixelsSizeX(index).getNumberValue().intValue();
          int sizeY =
            metadata.getPixelsSizeY(index).getNumberValue().intValue();
          if (descriptor.sizeX != sizeX) {
            throw new FormatException(String.format(
                "Resolution %d dimension mismatch! metadata=%d pyramid=%d",
                resolution, descriptor.sizeX, sizeX));
          }
          if (descriptor.sizeY != sizeY) {
            throw new FormatException(String.format(
                "Resolution %d dimension mismatch! metadata=%d pyramid=%d",
                resolution, descriptor.sizeY, sizeY));
          }
        }

        if (dimensions.length != 5) {
          throw new FormatException(String.format(
            "Expected 5 dimensions in series %d, found %d",
            index, dimensions.length));
        }
        for (int i=0; i<dimensions.length-2; i++) {
          if (dimensions[i] != dimensionLengths[2 - i]) {
            throw new FormatException(
              "Dimension order mismatch in series " + index);
          }
        }
      }

      resolutions.add(descriptor);
    }
  }

  /**
   * Convenience method that delegates to FormatTools to calculate
   * the Z, C, and T index for a given plane index.
   *
   * @param plane index
   * @return array of Z, C, and T indexes
   */
  public int[] getZCTCoords(int plane) {
    return FormatTools.getZCTCoords(dimensionOrder, z, c, t, planeCount, plane);
  }

  /**
   * Calculate the number of tiles for a dimension based upon the tile size.
   *
   * @param size the number of pixels in the dimension (e.g. image width)
   * @param tileSize the number of pixels in the tile along the same dimension
   * @return the number of tiles
   */
  private int getTileCount(long size, long tileSize) {
    return (int) Math.ceil((double) size / tileSize);
  }
}
