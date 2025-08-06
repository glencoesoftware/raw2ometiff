[![Build status](https://github.com/glencoesoftware/raw2ometiff/workflows/Gradle/badge.svg))](https://github.com/glencoesoftware/raw2ometiff/actions)

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

Configuring Logging
===================

Logging is provided using the logback library. The `logback.xml` file in `src/dist/lib/config/` provides a default configuration for the command line tool.
In release and snapshot artifacts, `logback.xml` is in `lib/config/`.
You can configure logging by editing the provided `logback.xml` or by specifying the path to a different file:

    JAVA_OPTS="-Dlogback.configurationFile=/path/to/external/logback.xml" \
    raw2ometiff ...

Alternatively you can use the `--debug` flag, optionally writing the stdout to a file:

    raw2ometiff /path/to/zarr-pyramid /path/to/file.ome.tiff --debug > raw2ometiff.log

The `--log-level` option takes an [slf4j logging level](https://www.slf4j.org/faq.html#fatal) for additional simple logging configuration.
`--log-level DEBUG` is equivalent to `--debug`. For even more verbose logging:

    raw2ometiff /path/to/zarr-pyramid /path/to/file.ome.tiff --log-level TRACE

Eclipse Configuration
=====================

1. Run the Gradle Eclipse task:

    ./gradlew eclipse

2. Add the logback configuration in `src/dist/lib/config/` to your CLASSPATH.

Usage
=====

Run the conversion (Bio-Formats 6.x):

    raw2ometiff tile_directory pyramid.ome.tiff

or generate a 5.9.x-compatible pyramid:

    raw2ometiff tile_directory pyramid.tiff --legacy

The input tile directory must contain a full pyramid in a Zarr container.

If the `--rgb` option is set, the data will be stored in the OME-TIFF using
the [chunky format](https://web.archive.org/web/20240528191227/https://www.awaresystems.be/imaging/tiff/tifftags/planarconfiguration.html)
and the [RGB color space](https://web.archive.org/web/20240708194342/https://www.awaresystems.be/imaging/tiff/tifftags/photometricinterpretation.html),
as further described in the [TIFF specification](https://www.itu.int/itudoc/itu-t/com16/tiff-fx/docs/tiff6.pdf).
This option is strongly recommended when the input data is a brightfield whole slide.

Tile compression is performed in parallel.  The number of workers can be changed using the `--max_workers` option.

`axes` and `transformations` metadata in the input Zarr will be ignored. This metadata is assumed to be consistent
with the corresponding `PhysicalSize*`, `TimeIncrement`, and `DimensionOrder` values in the input `METADATA.ome.xml`.

Compression type and quality
============================

The compression can be changed using the `--compression` option.
Valid compression types are `Uncompressed`, `LZW`, `JPEG-2000`, `JPEG-2000 Lossy`, `JPEG`, and `zlib`.
By default, `LZW` compression will be used in the OME-TIFF file, as this is a lossless compression type.

If the `--compression` option is set to `JPEG-2000 Lossy`, then
the `--quality` option can be used to control encoded bitrate in bits per pixel.
The quality is a floating point number and must be greater than 0. A larger number implies less data loss but also larger file size.
By default, the quality is set to the largest positive finite value of type double (64 bit floating point).
This is equivalent to lossless compression, i.e. setting `--compression` to `JPEG-2000`.
To see truly lossy compression, the quality should be set to less than the bit depth of the input image (e.g. less than 8 for uint8 data).
We recommend experimenting with different quality values between 0.25 and the bit depth of the input image to find an acceptable tradeoff
between file size and visual appeal of the converted images.

If the `--compression` option is set to `JPEG`, then
the `--quality` option controls the compression quality on a scale from `0.25` (25%, extremely lossy)
to `1.0` (100%, nearly lossless). The default value is `0.75`.

License
=======

The converter is distributed under the terms of the GPL license.
Please see `LICENSE.txt` for further details.
