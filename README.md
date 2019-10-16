[![Build status](https://ci.appveyor.com/api/projects/status/hvqqnbiwmo90m2fd?svg=true)](https://ci.appveyor.com/project/gs-jenkins/raw-to-ome-tiff)

raw-to-ome-tiff converter
=========================

Java application to convert a directory of tiles to an OME-TIFF pyramid.
This is the second half of iSyntax => OME-TIFF conversion.


Usage
=====

Build with Gradle:

    gradle clean build

Unpack the distribution:

    cd build/distributions
    unzip raw-to-ome-tiff-$VERSION.zip
    cd raw-to-ome-tiff-$VERSION

Run the conversion:

    bin/raw-to-ome-tiff tile_directory --output pyramid.ome.tiff


Areas to improve
================

* Add compression option
* Try faster writing option (TiffSaver instead of PyramidOMETiffWriter)
    - this is little more complicated since the tiles are RGB
* Add option to generate subresolutions instead of reading from disk
