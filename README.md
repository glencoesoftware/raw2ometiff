[![Build status](https://ci.appveyor.com/api/projects/status/hvqqnbiwmo90m2fd?svg=true)](https://ci.appveyor.com/project/gs-jenkins/raw2ometiff)

raw2ometiff converter
=====================

Java application to convert a directory of tiles to an OME-TIFF pyramid.
This is the second half of iSyntax/.mrxs => OME-TIFF conversion.

Requirements
============

libblosc (https://github.com/Blosc/c-blosc) version 1.9.0 or later must be installed separately.
The native libraries are not packaged with any relevant jars.  See also note in n5-zarr readme (https://github.com/saalfeldlab/n5-zarr/blob/0.0.2-beta/README.md)

Usage
=====

Build with Gradle:

    gradle clean build

Unpack the distribution:

    cd build/distributions
    unzip raw2ometiff-$VERSION.zip
    cd raw2ometiff-$VERSION

Run the conversion (Bio-Formats 6.x):

    bin/raw2ometiff tile_directory pyramid.ome.tiff

or generate a 5.9.x-compatible pyramid:

    bin/raw2ometiff tile_directory pyramid.tiff --legacy

The input tile directory must contain a full pyramid in a Zarr or N5 container.

By default, LZW compression will be used in the OME-TIFF file.
The compression can be changed using the `--compression` option.
Tile compression is performed in parallel.  The number of workers can be changed using the `--max_workers` option.

Areas to improve
================

* Compatibility with both iSyntax and .mrxs
    - map JSON metadata to OME-XML (magnification etc.)
    - fix tile sizes used when downsampling (TIFF input only)
