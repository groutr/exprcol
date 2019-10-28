from collections import defaultdict, deque

from dask.optimization import cull
from dask.core import get as dcget

from . import REGISTRY, requires

__all__ = ('unresolved', 'compute')

def _generate_dask_graph(data, keys):
    """Generate a dask graph from a subset of REGISTRY.

    Arguments:
        data {[type]} -- [description]
        keys {[type]} -- [description]
    """
    tasks = {k: data[k] for k in unresolved(keys)}
    tasks.update(REGISTRY)
    return cull(tasks, keys)[0]


def compute(data, cols=None):
    if cols is None:
        cols = []
    else:
        cols = list(set(cols))

    dsk = _generate_dask_graph(data, cols)
    return dict(zip(cols, dcget(dsk, cols)))


def unresolved(cols):
    cols = set(cols)
    while True:
        overlap = REGISTRY.keys() & cols
        if not overlap:
            break

        for c in overlap:
            cols.update(requires(REGISTRY[c]))
            cols.remove(c)
    return cols
