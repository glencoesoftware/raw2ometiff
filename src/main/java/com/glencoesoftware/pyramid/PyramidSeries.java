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
import loci.formats.ome.OMEPyramidStore;
import loci.formats.tiff.IFDList;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PyramidSeries {

  private static final Logger LOG =
    LoggerFactory.getLogger(PyramidSeries.class);

  /** Path to series in N5/Zarr. */
  String path;

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
   * @param n5Reader reader used to get dataset attributes
   * @param metadata additional OME-XML metadata
   */
  public void describePyramid(N5FSReader n5Reader, OMEPyramidStore metadata)
    throws FormatException, IOException
  {
    LOG.info("Number of resolution levels: {}", numberOfResolutions);
    resolutions = new ArrayList<ResolutionDescriptor>();
    for (int resolution = 0; resolution < numberOfResolutions; resolution++) {
      ResolutionDescriptor descriptor = new ResolutionDescriptor();
      descriptor.resolutionNumber = resolution;
      descriptor.path = path + "/" + resolution;

      DatasetAttributes attrs = n5Reader.getDatasetAttributes(descriptor.path);
      descriptor.sizeX = (int) attrs.getDimensions()[0];
      descriptor.sizeY = (int) attrs.getDimensions()[1];
      descriptor.tileSizeX = attrs.getBlockSize()[0];
      descriptor.tileSizeY = attrs.getBlockSize()[1];
      descriptor.numberOfTilesX =
        getTileCount(descriptor.sizeX, descriptor.tileSizeX);
      descriptor.numberOfTilesY =
        getTileCount(descriptor.sizeY, descriptor.tileSizeY);

      if (resolution == 0) {
        // If we have image metadata available sanity check the dimensions
        // against those in the underlying N5 pyramid
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

        long[] lengths = attrs.getDimensions();
        if (lengths.length != 5) {
          throw new FormatException(String.format(
            "Expected 5 dimensions in series %d, found %d",
            index, lengths.length));
        }
        for (int i=2; i<lengths.length; i++) {
          if ((int) lengths[i] != dimensionLengths[i - 2]) {
            throw new FormatException(
              "Dimension order mismatch in series " + index);
          }
        }
      }

      resolutions.add(descriptor);
    }
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
