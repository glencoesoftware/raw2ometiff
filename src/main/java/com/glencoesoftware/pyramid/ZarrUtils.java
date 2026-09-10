/**
 * Copyright (c) 2026 Glencoe Software, Inc. All rights reserved.
 *
 * This software is distributed under the terms described by the LICENSE.txt
 * file you can find at the root of the distribution bundle.  If the file is
 * missing please request a copy by contacting info@glencoesoftware.com
 */
package com.glencoesoftware.pyramid;

import java.util.List;
import java.util.Map;

import com.glencoesoftware.bioformats2raw.SupportedVersions;

import loci.formats.FormatException;

public class ZarrUtils {

  /**
   * Get a List representing the "axes" attribute nested under
   * an element of the "multiscales" array. The location of the "axes"
   * attribute depends upon the OME-Zarr version that was written.
   *
   * @param multiscale Map representing an element of the "multiscales" array
   * @param version OME-Zarr version
   * @return List representing the axes
   */
  public static List<Map<String, Object>> getAxes(
    Map<String, Object> multiscale, SupportedVersions version)
  {
    if (version.supportsCoordinateSystems()) {
      List<Map<String, Object>> coordinateSystems =
        (List<Map<String, Object>>) multiscale.get("coordinateSystems");
      return (List<Map<String, Object>>) coordinateSystems.get(0).get("axes");
    }
    return (List<Map<String, Object>>) multiscale.get("axes");
  }

  /**
   * Get an enum instance representing the given version string.
   *
   * @param v OME-Zarr version string (e.g. "0.4")
   * @return corresponding instance of SupportedVersions enum
   * @throws FormatException if the version string does not have
   *                         a corresponding enum value
   */
  public static SupportedVersions getOMEZarrVersion(String v)
    throws FormatException
  {
    for (SupportedVersions s : SupportedVersions.class.getEnumConstants()) {
      if (s.toString().equals(v)) {
        return s;
      }
    }
    throw new FormatException("Unsupported OME-Zarr version: " + v);
  }

}
