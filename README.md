[![Build status](https://ci.appveyor.com/api/projects/status/hvqqnbiwmo90m2fd?svg=true)](https://ci.appveyor.com/project/gs-jenkins/raw2ometiff)

raw2ometiff converter
=====================

Java application to convert a directory of tiles to an OME-TIFF pyramid.
This is the second half of iSyntax/.mrxs => OME-TIFF conversion.

Requirements
============

libblosc (https://github.com/Blosc/c-blosc) version 1.9.0 or later must be installed separately.
The native libraries are not packaged with any relevant jars.  See also note in n5-zarr readme (https://github.com/saalfeldlab/n5-zarr/blob/0.0.2-beta/README.md)

 * Mac OSX: `brew install c-blosc`
 * Ubuntu 18.04+: `apt-get install libblosc1`

Installation
============

1. Download and unpack a release artifact:

    https://github.com/glencoesoftware/raw2ometiff/releases

Development Installation
========================

1. Clone the repository:

    git clone git@github.com:glencoesoftware/raw2ometiff.git

2. Run the Gradle build as required, a list of available tasks can be found by running:

    ./gradlew tasks

Eclipse Configuration
=====================

1. Run the Gradle Eclipse task:

    ./gradlew eclipse

Usage
=====

Run the conversion (Bio-Formats 6.x):

    raw2ometiff tile_directory pyramid.ome.tiff

or generate a 5.9.x-compatible pyramid:

    raw2ometiff tile_directory pyramid.tiff --legacy

The input tile directory must contain a full pyramid in a Zarr or N5 container.

By default, LZW compression will be used in the OME-TIFF file.
The compression can be changed using the `--compression` option.
Tile compression is performed in parallel.  The number of workers can be changed using the `--max_workers` option.

Areas to improve
================

* Compatibility with both iSyntax and .mrxs
    - map JSON metadata to OME-XML (magnification etc.)
    - fix tile sizes used when downsampling (TIFF input only)

License
=======

The converter is distributed under the terms of the GPL license.
Please see `LICENSE.txt` for further details.
