[![Build status](https://ci.appveyor.com/api/projects/status/hvqqnbiwmo90m2fd?svg=true)](https://ci.appveyor.com/project/gs-jenkins/raw2ometiff)

raw2ometiff converter
=====================

Java application to convert a directory of tiles to an OME-TIFF pyramid.
This is the second half of iSyntax/.mrxs => OME-TIFF conversion.

Requirements
============

Java 8 or later is required.

libblosc (https://github.com/Blosc/c-blosc) version 1.9.0 or later must be installed separately.
The native libraries are not packaged with any relevant jars.  See also note in jzarr readme (https://github.com/bcdev/jzarr/blob/master/README.md)

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

The input tile directory must contain a full pyramid in a Zarr container.

The compression can be changed using the `--compression` option.
Valid compression types are `Uncompressed`, `LZW`, `JPEG-2000`, `JPEG-2000 Lossy`, `JPEG`, and `zlib`.
By default, `LZW` compression will be used in the OME-TIFF file.

If the `--compression` option is set to `JPEG-2000 Lossy`, then
the `--quality` option can be used to control encoded bitrate in bits per pixel.
The quality is a floating point number and must be greater than 0. A larger number implies less data loss but also larger file size.
By default, the quality is set to the largest positive finite value of type double (64 bit floating point).
This is equivalent to lossless compression, i.e. setting `--compression` to `JPEG-2000`.
To see truly lossy compression, the quality should be set to less than the bit depth of the input image (e.g. less than 8 for uint8 data).
We recommend experimenting with different quality values between 0.25 and the bit depth of the input image to find an acceptable tradeoff
between file size and visual appeal of the converted images.

Tile compression is performed in parallel.  The number of workers can be changed using the `--max_workers` option.

`axes` and `transformations` metadata in the input Zarr will be ignored. This metadata is assumed to be consistent
with the corresponding `PhysicalSize*`, `TimeIncrement`, and `DimensionOrder` values in the input `METADATA.ome.xml`.

Areas to improve
================

* Compatibility with both iSyntax and .mrxs
    - map JSON metadata to OME-XML (magnification etc.)
    - fix tile sizes used when downsampling (TIFF input only)

License
=======

The converter is distributed under the terms of the GPL license.
Please see `LICENSE.txt` for further details.
