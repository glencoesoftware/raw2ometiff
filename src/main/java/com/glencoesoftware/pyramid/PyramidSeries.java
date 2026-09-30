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
import java.util.Map;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.Modulo;
import loci.formats.ome.OMEPyramidStore;
import loci.formats.tiff.IFDList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.glencoesoftware.bioformats2raw.Axis;
import com.glencoesoftware.bioformats2raw.SupportedVersions;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Array;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.core.Group;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.utils.Utils;

public class PyramidSeries {

  private static final Logger LOG =
    LoggerFactory.getLogger(PyramidSeries.class);

  SupportedVersions version;

  /** Path to series. */
  String path;

  List<String> uuid = new ArrayList<String>();

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
   * @param store store used to get dataset attributes
   * @param metadata additional OME-XML metadata
   * @param moduloZ ModuloAlongZ
   * @param moduloC ModuloAlongC
   * @param moduloT ModuloAlongT
   */
  public void describePyramid(FilesystemStore store, OMEPyramidStore metadata,
    Modulo moduloZ, Modulo moduloC, Modulo moduloT)
    throws FormatException, IOException
  {
    LOG.info("Number of resolution levels: {}", numberOfResolutions);

    List<Map<String, Object>> multiscales = null;

    try {
      Group subgroup = Group.open(store.resolve(path));
      Attributes attrs = subgroup.metadata().attributes();
      if (subgroup instanceof dev.zarr.zarrjava.v3.Group) {
        Attributes ome = attrs.getAttributes("ome");
        multiscales = (List<Map<String, Object>>) ome.get("multiscales");
      }
      else {
        multiscales = (List<Map<String, Object>>) attrs.get("multiscales");
      }
    }
    catch (ZarrException e) {
      throw new FormatException(e);
    }

    resolutions = new ArrayList<ResolutionDescriptor>();
    for (int resolution = 0; resolution < numberOfResolutions; resolution++) {
      ResolutionDescriptor descriptor = new ResolutionDescriptor();
      descriptor.path = String.valueOf(resolution);
      descriptor.moduloZ = moduloZ;
      descriptor.moduloC = moduloC;
      descriptor.moduloT = moduloT;
      if (!path.isEmpty()) {
        descriptor.path = path + "/" + descriptor.path;
      }
      try {
        Array array = Array.open(store.resolve(descriptor.path));
        int[] shape = Utils.toIntArray(array.metadata().shape);
        int[] chunk = array.metadata().chunkShape();
        descriptor.parseMultiscales(multiscales, shape, version);
        setupResolution(descriptor, resolution, shape, chunk, metadata);
      }
      catch (ZarrException e) {
        throw new FormatException(e);
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
    int effectiveC = rgb ? c / 3 : c;
    return FormatTools.getZCTCoords(
      dimensionOrder, z, effectiveC, t, planeCount, plane);
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

  private void setupResolution(ResolutionDescriptor descriptor, int resolution,
    int[] dimensions, int[] blockSizes, OMEPyramidStore metadata)
    throws FormatException
  {
    descriptor.resolutionNumber = resolution;

    int xIndex = descriptor.getIndex("X");
    int yIndex = descriptor.getIndex("Y");

    descriptor.sizeX = dimensions[xIndex];
    descriptor.sizeY = dimensions[yIndex];
    descriptor.tileSizeX = blockSizes[xIndex];
    descriptor.tileSizeY = blockSizes[yIndex];

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

      Modulo mz = descriptor.moduloZ;
      Modulo mc = descriptor.moduloC;
      Modulo mt = descriptor.moduloT;

      int[] total = new int[] {1, 1, 1};
      for (int i=0; i<descriptor.axes.size(); i++) {
        Axis axis = descriptor.axes.get(i);
        String axisName = axis.getType();
        if (axisName.equalsIgnoreCase("X") || axisName.equalsIgnoreCase("Y")) {
          continue;
        }

        // the "type" attribute of a Modulo may be "other", since this is parsed
        // from an OME-XML annotation which enforces the enum documented in
        // https://ome-model.readthedocs.io/en/latest/developers/
        // this means the "type" attribute of the Modulo may not match the
        // "name" attribute of the Zarr axis metadata
        // instead assume that a modulo axis' Zarr metadata "type" is set to
        // match the "type" of the parent axis, e.g. Zarr axis "C" and the Zarr
        // axis representing ModuloAlongC will have Zarr axis "type"
        // set to "channel"
        String axisType = axis.getDimensionType();
        int len = axis.getLength();
        if (axisName.equals("Z") ||
          (mz != null && axisType.equalsIgnoreCase("space") &&
          mz.length() == len))
        {
          total[0] *= len;
        }
        else if (axisName.equals("C") ||
          (mc != null && axisType.equalsIgnoreCase("channel") &&
          mc.length() == len))
        {
          total[1] *= len;
        }
        else if (axisName.equals("T") ||
          (mt != null && axisType.equalsIgnoreCase("time") &&
          mt.length() == len))
        {
          total[2] *= len;
        }
      }
      for (int i=0; i<dimensionLengths.length; i++) {
        // dimensionLengths is in ZCT order, independent of dimensionOrder
        String axis = "ZCT".substring(i, i + 1);
        if (dimensionLengths[i] != total[i]) {
          throw new FormatException(
            "Mismatch on dimension " + axis + "; expected " +
            total[i] + ", got " + dimensionLengths[i]);
        }
      }
    }
  }

}
