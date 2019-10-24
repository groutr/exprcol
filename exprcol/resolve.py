from collections import defaultdict, deque

import numpy as np

from . import REGISTRY, requires

__all__ = ('unresolved', 'compute')

def slice_graph(keys):
    sliced = set()
    children = deque(keys)
    while children:
        node = children.popleft()
        sliced.add(node)
        reqs = REGISTRY.get(node, None)
        if reqs:
            children.extend(requires(reqs))
    return sliced


def toposort(G):
    # From NetworkX

    seen = set()
    order = []
    explored = set()

    for n in G:
        if n in explored:
            continue
        fringe = [n]
        while fringe:
            w = fringe[-1]
            if w in explored:
                fringe.pop()
                continue
            seen.add(w)

            new_nodes = []
            for v in G[w]:
                if v not in explored:
                    if v in seen:
                        raise ValueError("Graph has a cycle")
                    new_nodes.append(v)

            if new_nodes:
                fringe.extend(new_nodes)
            else:
                explored.add(w)
                order.append(w)
                fringe.pop()
    return tuple(reversed(order))


def compute(data, cols=None, only_cols=True):
    if cols is None:
        cols = set()
    else:
        cols = set(cols)

    data = {c: np.asanyarray(data[c]) for c in unresolved(cols)}
    cached = {}
    for node in toposort(cols):
        if node in data:
            cached[node] = data[node]
            continue

        func, args = REGISTRY[node]
        data[node] = func(*(cached[c] for c in args))

    if only_cols:
        rv_cols = cached.keys() & cols
    else:
        rv_cols = cached.keys() - data.keys()
    return {k: cached[k] for k in rv_cols}


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
