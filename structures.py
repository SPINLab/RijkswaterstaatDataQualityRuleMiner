#! /usr/bin/env python

from re import fullmatch
from uuid import uuid4

from rdflib.term import Node

from timeutils import days_to_date


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
    children = None  # children Clause instances; used for validation optimization

    _prune = False
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
        self.children = set()

        self._prune = False
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

    def __lt__(self, other):
        return self.type < other.type

    def __hash__(self):
        return hash(str(self.__class__.__name__)+str(self.type))

    def __str__(self):
        return "TYPE [{}]".format(str(self.type))

    def __repr__(self):
        return "TypeVariable {} [{}]".format(str(id(self)),
                                             str(self))


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
        return "ObjectTypeVariable {} [{}]".format(str(id(self)),
                                                   str(self))


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

class MultiModalNode(TypeVariable):
    """ Multimodal Node class """
    def __init__(self, type):
        super().__init__(type)

    def __str__(self):
        return "MULTIMODAL [{}]".format(str(self.type))

    def __repr__(self):
        return "MultiModalNode {} [{}]".format(str(id(self)),
                                             str(self))

class MultiModalNumericNode(MultiModalNode):
    """ Numeric Node class """
    min = 0.0
    max = 0.0

    def __init__(self, type, min, max):
        super().__init__(type)
        self.min = min
        self.max = max

    def __eq__(self, other):
        return type(self) is type(other)\
                and self.type is other.type\
                and self.min == other.min\
                and self.max == other.max

    def __lt__(self, other):
        return self.min < other.min\
                or (self.min == other.min and self.max < other.max)

    def __contains__(self, value):
        return value >= self.min and value <= self.max

    def __hash__(self):
        return hash(str(self.__class__.__name__)+str(self.type)
                    +str(self.min)+str(self.max))

    def __str__(self):
        return "Numeric ({},{})".format(str(self.min),
                                        str(self.max))

    def __repr__(self):
        return "MultiModalNode {} {}".format(str(id(self)),
                                             str(self))

class MultiModalStringNode(MultiModalNode):
    """ String Node class """
    regex = ""

    def __init__(self, type, regex):
        super().__init__(type)
        self.regex = regex

    def __eq__(self, other):
        # does not account for equivalent regex patterns
        return type(self) is type(other)\
                and self.type is other.type\
                and self.regex == other.regex

    def __lt__(self, other):
        # this should idealy be that one regex pattern is less constrained than
        # the other.
        return len(self.regex) < len(other.regex)

    def __contains__(self, value):
        return fullmatch(self.regex, value) is not None

    def __hash__(self):
        return hash(str(self.__class__.__name__)+str(self.type)
                    +self.regex)

    def __str__(self):
        return "String ({})".format(self.regex)

    def __repr__(self):
        return "MultiModalNode {} {}".format(str(id(self)),
                                             str(self))

class MultiModalDateTimeNode(MultiModalNode):
    """ Date Time Node class """
    begin = 0.0
    end = 0.0

    def __init__(self, type, begin, end):
        super().__init__(type)
        self.begin = begin
        self.end = end

    def __eq__(self, other):
        return type(self) is type(other)\
                and self.type is other.type\
                and self.begin == other.begin\
                and self.end == other.end

    def __lt__(self, other):
        return self.begin < other.begin\
                or (self.begin == other.begin and self.end < other.end)

    def __contains__(self, value):
        return value >= self.begin and value <= self.end

    def __hash__(self):
        return hash(str(self.__class__.__name__)+str(self.type)
                    +str(self.begin)+str(self.end))

    def __str__(self):
        return "DateTime ({},{})".format(str(self.begin),
                                         str(self.end))

    def __repr__(self):
        return "MultiModalNode {} {}".format(str(id(self)),
                                             str(self))

class MultiModalDateFragNode(MultiModalDateTimeNode):
    """ Date Fragment Node class """
    gBegin = 0
    gEnd = 0

    def __init__(self, type, begin, end):
        # begin and end are in number of days
        super().__init__(type, begin, end)
        self.gBegin = days_to_date(begin, type)
        self.gEnd = days_to_date(end, type)

    def __str__(self):
        return "DateFrag ({},{})".format(str(self.gBegin),
                                         str(self.gEnd))

    def __repr__(self):
        return "MultiModalNode {} {}".format(str(id(self)),
                                             str(self))

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
        copy = Assertion(self.lhs, self.predicate, self.rhs)
        if not reset_uuid:
            copy._uuid = self._uuid

        return copy

    def __getnewargs__(self):
        return (self.lhs, self.predicate, self.rhs)

    def __hash__(self):
        # require unique hash to prevent overlapping dict keys
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
        copy = IdentityAssertion(self.lhs, self.predicate, self.rhs)
        if not reset_uuid:
            copy._uuid = self._uuid

class ClauseBody():
    """ Clause Body class

    Holds all assertions of a clause's body (set of constraints) and keeps
    track of the connections and distances (from the root) of these assertions.
    """
    connections = None
    distances = None
    _distances_reverse = None
    identity = None

    def __init__(self, identity, connections=None, distances=None, distances_reverse=None):
        if not isinstance(identity, Assertion):
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
        if not isinstance(endpoint, Assertion) or\
           not isinstance(extension, Assertion):
            raise TypeError()

        self.connections[endpoint].add(extension)
        self.connections[extension] = set()  # Assertion instances have unique hashes

        distance = self._distances_reverse[endpoint] + 1
        self._distances_reverse[extension] = distance
        if distance not in self.distances.keys():
            self.distances[distance] = set()
        self.distances[distance].add(extension)

    def copy(self):
        return ClauseBody(connections={k:{v for v in self.connections[k]} for k in self.connections.keys()},
                           distances={k:{v for v in self.distances[k]} for k in self.distances.keys()},
                           distances_reverse={k:v for k,v in self._distances_reverse.items()},
                           identity=self.identity)

    def __hash__(self):
        # order invariant
        value = str(self.__class__.__name__) + str(self.identity) + str(self) +\
                "".join([str(k)+"".join(["".join(v) for v in sorted(self.distances[k])])
                         for k in sorted(self.distances.keys())])

        return hash(value)

    def __len__(self):
        return len(self.connections.keys())

    def __repr__(self):
        return "BODY [{}]".format(str(self))

    def __str__(self):
        return "{" + "; ".join([str(assertion) for assertion in
                                self.connections.keys()]) + "}"


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

    def get(self, ctype=None, depth=-1):
        if ctype is None:
            for ctype in self._trees.keys():
                for clause in self._trees[ctype].get(depth):
                    yield clause
            return

        if ctype not in self._trees.keys():
            raise KeyError()

        return self._trees[ctype].get(depth)

    def get_tree(self, ctype):
        if ctype not in self._trees.keys():
            raise KeyError()

        return self._trees[ctype]

    def prune(self, ctype, depth, clauses):
        if ctype not in self._trees.keys():
            raise KeyError()

        self._trees[ctype].prune(clauses, depth)

    def clear(self, ctype, depth):
        if ctype not in self._trees.keys():
            raise KeyError()

        self._trees[ctype].clear(depth)

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

    def add(self, clause, depth):
        if type(clause) is not Clause:
            raise TypeError()
        if depth > self.height:
            raise IndexError("Depth exceeds height of tree")
        if self.height <= depth:
            self._tree.append(set())
            self.height += 1

        self._tree[depth].add(clause)
        self.size += 1

    def rmv(self, clause, depth):
        if type(clause) is not Clause:
            raise TypeError()
        if depth >= self.height:
            raise IndexError("Depth exceeds height of tree")

        self._tree[depth].remove(clause)
        self.size -= 1

    def update(self, clauses, depth):
        # redundancy needed for case if len(clauses) == 0
        if depth > self.height:
            raise IndexError("Depth exceeds height of tree")
        if self.height <= depth:
            self._tree.append(set())
            self.height += 1

        for clause in clauses:
            self.add(clause, depth)

    def prune(self, clauses, depth):
        for clause in clauses:
            self.rmv(clause, depth)

    def clear(self, depth):
        if depth >= self.height:
            raise IndexError("Depth exceeds height of tree")

        self.size -= len(self._tree[depth])
        self._tree[depth] = set()

    def get(self, depth=-1):
        if depth < 0:
            if len(self._tree) > 0:
                for clause in set.union(*self._tree):
                    yield clause
        else:
            if depth >= self.height:
                raise IndexError("Depth exceeds height of tree")

            for clause in self._tree[depth]:
                yield clause

    def __len__(self):
        return self.height

    def __str__(self):
        return "{}:{}".format(self.height, self.size)
