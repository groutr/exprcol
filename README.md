Expression Columns for tabular datasets.

A small library for defining and computing columns defined by an expression.
Benefits:
  1. Can be faster than naive computation because intermediate results are reused
  2. Avoids scattering computation details inside codebase.


See example notebook for usage.

This library was reimplemented to use dask-core for graph manipulations.
