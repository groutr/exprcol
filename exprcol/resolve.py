from dask.optimization import cull
from dask.core import get as dcget

from . import REGISTRY, requires

__all__ = ('unresolved', 'compute')

def _generate_dask_graph(data, keys):
    """
    Generate a dask graph from a subset of REGISTRY.
    """
    tasks = cull(REGISTRY, keys)[0]
    for k in unresolved(tasks, keys):
        tasks[k] = data[k]
    return tasks


def compute(data, cols=None):
    """stats from counter data
    Compute requested expressions from data

    Arguments:
        data: Data to compute on. Assumed to a be a key/value data structure
        cols: Column expressions to compute.

    Returns:
        (dict): Column expression results.
    """
    if cols is None:
        cols = []
    else:
        cols = list(set(cols))

    dsk = _generate_dask_graph(data, cols)
    return dict(zip(cols, dcget(dsk, cols)))


def unresolved(subgraph, cols):
    result = set()
    cols = list(cols)
    while cols:
        c = cols.pop()
        if c in subgraph:
            cols.extend(requires(subgraph[c]))
        else:
            if isinstance(c, str):
                result.add(c)
    return result