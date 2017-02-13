# suffix-arrays
Spark-based implementation of pDC3, a linear-time parallel suffix-array-construction algorithm.

[![Build Status](https://travis-ci.org/ryan-williams/suffix-arrays.svg?branch=master)](https://travis-ci.org/ryan-williams/suffix-arrays)
[![Coverage Status](https://coveralls.io/repos/github/ryan-williams/suffix-arrays/badge.svg?branch=master)](https://coveralls.io/github/ryan-williams/suffix-arrays?branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/org.hammerlab/suffix-arrays_2.11.svg?maxAge=600)](http://search.maven.org/#search%7Cga%7C1%7Corg.hammerlab%20suffix-arrays)

This repo contains:
- an implementation of the sequential algorithm DC3 ([paper](http://algo2.iti.kit.edu/documents/jacm05-revised.pdf)) under [`org.hammerlab.suffixes.dc3`](src/main/scala/org/hammerlab/suffixes/dc3).
- an Apache-Spark-based implementation of its parallel counterpart, pDC3 ([paper](http://algo2.iti.kit.edu/sanders/papers/KulSan06a.pdf)), under [`org.hammerlab.suffixes.pdc3`](src/main/scala/org/hammerlab/suffixes/pdc3).

[The tests](src/test/scala/org/hammerlab/suffixes) verify that both give the same answers on a variety of inputs.
