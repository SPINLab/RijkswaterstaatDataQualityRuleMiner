#! /usr/bin/env python

from rdflib.term import Node


class Clause():
    probability = 0.0  # double
    head = None  # tuple entity (URIRef), predicate (URIRef), entity/literal (RDFNode)
    body = None  # set of triple tuples
    parent = None  # parent Clause instance

    _satisfy_body = None
    _satisfy_full = None

    def __init__(self, head, body, probability=0.0, parent=None):
        self.head = head
        self.body = body
        self.probability = probability
        self.parent = parent

        self._satisfy_body = set()
        self._satisfy_full = set()

    def __len__(self):
        return len(self.body)

    def __str__(self):
        return "{:0.3f}: {} <- {{}}".format(self.probability,
                                       self.head,
                                       str(self.body))

    def __repr__(self):
        return "Clause [{}]".format(str(self))


    class TypeVariable(Node):
        type = None

        def __init__(self, type):
            self.type = type
            super().__init__()

        def __str__(self):
            return "TYPE ({})".format(self.type)

        def __repr__(self):
            return "TypeVariable [{}]".format(str(self))


    class ObjectTypeVariable(TypeVariable):
        def __init__(self, type):
            super().__init__(type)

        def __str__(self):
            return "OBJECT TYPE ({})".format(self.type)

        def __repr__(self):
            return "ObjectTypeVariable [{}]".format(str(self))


    class DataTypeVariable(TypeVariable):
        def __init__(self, type):
            super().__init__(type)

        def __str__(self):
            return "DATA TYPE ({})".format(self.type)

        def __repr__(self):
            return "DataTypeVariable [{}]".format(str(self))


class GenerationForest():
    _trees = None

    def __init__(self):
        self._trees = dict()

    def add(self, ctype, depth, clause):
        if ctype not in self._trees.keys():
            raise KeyError()

        self._trees[ctype].add(clause, depth)

    def get(self, ctype, depth):
        if ctype not in self._trees.keys():
            raise KeyError()

        return self._trees[ctype].get(depth)

    def get_tree(self, ctype):
        if ctype not in self._trees.keys():
            raise KeyError()

        return self._trees[ctype]

    def plant(self, ctype, tree):
        if type(tree) is not GenerationTree:
            raise TypeError()

        self._trees[ctype] = tree

    def types(self):
        return self._trees.keys()

    def __len__(self):
        return len(self._trees)

    def __str__(self):
        return "; ".join({"{} ({})".format(t, str(self.get_tree(t))) for t in self._trees.keys()})


class GenerationTree():
    height = -1  # number of levels
    size = -1  # number of vertices

    _tree = None

    def __init__(self):
        self._tree = list()
        self.height = 0
        self.size = 0

    def add(self, clause, depth, predicate=None):
        if type(clause) is not Clause:
            raise TypeError()
        if depth > self.height:
            raise IndexError()
        if self.height <= depth:
            self._tree.append(dict())
            self.height += 1

        p = clause.head[1] if predicate is None else predicate # predicate
        if p not in self._tree[depth].keys():
            self._tree[depth][p] = set()

        self._tree[depth][p].add(clause)
        self.size += 1

    def get(self, depth=-1, predicate=None):
        results = set()
        if depth < 0:
            if predicate is None:
                results = set.union(*[set.union(*d.values()) for d in self._tree])
            else:
                for d in self._tree:
                    if predicate in self._tree[d].keys():
                        results |= self._tree[d][predicate]
        else:
            if depth >= self.height:
                raise IndexError()
            if predicate is None:
                results = set.union(*self._tree[depth].values())
            else:
                results = self._tree[depth][predicate]

        return results

    def __len__(self):
        return self.height

    def __str__(self):
        return "{}:{}".format(self.height, self.size)
