#! /usr/bin/env python

from uuid import uuid4

from rdflib.term import Node


class Clause():
    """ Clause class

    A clause consists of a head (Assertion) which holds, with probability Pd and
    Pr, for all members of a type t if these members satisfy the constraints in
    the body (Body). Keeps track of its parent and of all members it satisfies
    for efficient computation of support and confidence.
    """
    domain_probability = 0.0  # probability that an arbitrary member of the
                              # domain satisfies the head
    range_probability = 0.0  # probability that an arbitrary member of the
                             # domain which satisfies the head's predicate also
                             # satisfies the object in the head
    support = 0  # number of members which satisfy the body
    confidence = 0  # number of members who satisfy both body and head
    head = None  # tuple (variable, predicate (URIRef), entity|literal|variable)
    body = None  # instance of Clause.Body
    parent = None  # parent Clause instance

    _satisfy_body = None
    _satisfy_full = None

    def __init__(self, head, body, domain_probability=0.0,
                 range_probability=0.0, confidence=0, support=0, parent=None):
        self.head = head
        self.body = body
        self.domain_probability = domain_probability
        self.range_probability = range_probability
        self.confidence = confidence
        self.parent = parent

        self._satisfy_body = set()
        self._satisfy_full = set()

    def __len__(self):
        return len(self.body)

    def __str__(self):
        return "[Pd:{:0.3f}, Pr:{:0.3f}, Supp:{}, Conf:{}] {} <- {{{}}}".format(
            self.domain_probability,
            self.range_probability,
            self.support,
            self.confidence,
            str(self.head),
            str(self.body))

    def __repr__(self):
        return "Clause [{}]".format(str(self))


    class TypeVariable(Node):
        """ Type Variable class

        An unbound variable which can take on any value of a certain object or
        data type resource
        """
        type = None

        def __init__(self, type):
            self.type = type
            super().__init__()

        def __eq__(self, other):
            return type(self) is type(other)\
                    and self.type is other.type

        def __str__(self):
            return "TYPE [{}]".format(str(self.type))

        def __repr__(self):
            return "TypeVariable [{}]".format(str(self))


    class ObjectTypeVariable(TypeVariable):
        """ Object Type Variable class

        An unbound variable which can be any member of an object type class
        (entity)
        """
        def __init__(self, type):
            super().__init__(type)

        def __str__(self):
            return "OBJECT TYPE [{}]".format(str(self.type))

        def __repr__(self):
            return "ObjectTypeVariable [{}]".format(str(self))


    class DataTypeVariable(TypeVariable):
        """ Data Type Variable class

        An unbound variable which can take on any value of a data type class
        (literal)
        """
        def __init__(self, type):
            super().__init__(type)

        def __str__(self):
            return "DATA TYPE ({})".format(str(self.type))

        def __repr__(self):
            return "DataTypeVariable [{}]".format(str(self))

    class Assertion(tuple):
        """ Assertion class

        Wrapper around tuple (an assertion) that gives each instantiation an
        unique uuid which allows for comparisons between assertions with the
        same values. This is needed when either lhs or rhs use TypeVariables.
        """
        lhs = None
        predicate = None
        rhs = None
        _uuid = None

        def __new__(cls, subject, predicate, object):
            return super().__new__(cls, (subject, predicate, object))

        def __init__(self, subject, predicate, object, _uuid=None):
            self.lhs = subject
            self.predicate = predicate
            self.rhs = object

            self._uuid = _uuid if _uuid is not None else uuid4()

        def copy(self, reset_uuid=True):
            copy = Clause.Assertion(self.lhs, self.predicate, self.rhs)
            if not reset_uuid:
                copy._uuid = self._uuid

            return copy

        def __getnewargs__(self):
            return (self.lhs, self.predicate, self.rhs)

        def __hash__(self):
            # require unique hash to prevent overlapping dictg keys
            return hash("".join([str(self.lhs), str(self.predicate),
                                 str(self.rhs), str(self._uuid)]))

    class IdentityAssertion(Assertion):
        """ Identity Assertion class

        Special class for identity assertion to allow for each recognition and
        selection.
        """
        def __new__(cls, subject, predicate, object):
            return super().__new__(cls, subject, predicate, object)

        def copy(self, reset_uuid=True):
            copy = Clause.IdentityAssertion(self.lhs, self.predicate, self.rhs)
            if not reset_uuid:
                copy._uuid = self._uuid

    class Body():
        """ Clause Body class

        Holds all assertions of a clause's body (set of constraints) and keeps
        track of the connections and distances (from the root) of these assertions.
        """
        connections = None
        distances = None
        _distances_reverse = None
        identity = None

        def __init__(self, identity, connections=None, distances=None, distances_reverse=None):
            if not isinstance(identity, Clause.Assertion):
                raise TypeError()

            self.identity = identity
            self.connections = connections
            self.distances = distances
            self._distances_reverse = distances_reverse

            if self.connections is None:
                self.connections = {identity: set()}
                self.distances = {0: {identity}}
                self._distances_reverse = {identity: 0}
                return

            if identity not in self.connections.keys():
                self.connections[identity] = set()
                self.distances[0].add(identity)
                self._distances_reverse[identity] = 0

        def extend(self, endpoint, extension):
            if not isinstance(endpoint, Clause.Assertion) or\
               not isinstance(extension, Clause.Assertion):
                raise TypeError()

            self.connections[endpoint].add(extension)
            self.connections[extension] = set()

            distance = self._distances_reverse[endpoint] + 1
            self._distances_reverse[extension] = distance
            if distance not in self.distances.keys():
                self.distances[distance] = set()
            self.distances[distance].add(extension)

        def copy(self):
            return Clause.Body(connections={k:{v for v in self.connections[k]} for k in self.connections.keys()},
                               distances={k:{v for v in self.distances[k]} for k in self.distances.keys()},
                               distances_reverse={k:v for k,v in self._distances_reverse.items()},
                               identity=self.identity)

        def __repr__(self):
            return "BODY [{}]".format(str(self))

        def __str__(self):
            return "{" + "; ".join({str(assertion) for assertion in
                                    self.connections.keys()}) + "}"


class GenerationForest():
    """ Generation Forest class

    Contains one or more generation trees (one per entity type) and serves as a
    wrapper for tree operations.
    """
    _trees = None

    def __init__(self):
        self._trees = dict()

    def add(self, ctype, depth, clause):
        if ctype not in self._trees.keys():
            raise KeyError()

        self._trees[ctype].add(clause, depth)

    def update_tree(self, ctype, clauses, depth):
        if ctype not in self._trees.keys():
            raise KeyError()

        self._trees[ctype].update(clauses, depth)

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
    """ Generation Tree class

    A mutitree consisting of all clauses that hold for entities of a certain
    type t. All clauses of depth 0 (body := {type(e, t)}) form the roots of the
    tree, with each additional depth consisting of one or more constraint
    clauses that expand their parents' body by one assertion.
    """
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

        p = clause.head.predicate if predicate is None else predicate # predicate
        if p not in self._tree[depth].keys():
            self._tree[depth][p] = set()

        self._tree[depth][p].add(clause)
        self.size += 1

    def update(self, clauses, depth):
        if depth > self.height:
            raise IndexError()
        if self.height <= depth:
            self._tree.append(dict())
            self.height += 1

        for clause in clauses:
            self.add(clause, depth)

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
                values = self._tree[depth].values()
                if len(values) > 0:
                    results = set.union(*values)
            else:
                results = self._tree[depth][predicate]

        return results

    def __len__(self):
        return self.height

    def __str__(self):
        return "{}:{}".format(self.height, self.size)
