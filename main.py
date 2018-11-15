#! /usr/bin/env python

from collections import Counter

from rdflib.namespace import RDF, XSD
from rdflib.graph import Literal, URIRef

from dijkstra import shortest_path
from structures import Clause, GenerationForest, GenerationTree


IGNORE_PREDICATES = {RDF.type}
IDENTITY = URIRef("local://identity")  # reflexive property

def generate(g, max_depth, min_support):
    generation_forest = init_generation_forest(g, min_support)

    for depth in range(0, max_depth):
        for ctype in generation_forest.types():
            derivatives = set()

            for clause in generation_forest.get(ctype, depth):
                # an attribute of an entity is already implicitly a constraint if
                # that entity itself is a constraint
                pendant_incidents = {assertion for assertion in clause.body.distances[depth]
                                        if type(assertion.rhs) is Clause.ObjectTypeVariable}
                derivatives |= explore(g, generation_forest, clause, pendant_incidents, min_support)

            generation_forest.add(ctype, derivatives, depth+1)

    return generation_forest

def explore(g, generation_forest, clause, pendant_incidents, min_support):
    extended_clauses = set()

    while len(pendant_incidents) > 0:
        assertion = pendant_incidents.pop()

        # gather all possible extensions for an entity of type t
        candidate_extensions = generation_forest.get(assertion.rhs.t, 0)
        extensions = extend(g, clause, assertion, candidate_extensions, min_support)
        extended_clauses |= extensions

        for extension in extended_clauses:
            extended_clauses |= explore(g, generation_forest, extension,
                                        pendant_incidents, min_support)

    return extended_clauses

def extend(g, clause, pendant_incident, candidate_extensions, min_support):
    extended_clauses = set()

    while len(candidate_extensions) > 0:
        candidate_extension = candidate_extensions.pop()

        body = clause.body.copy()
        body.extend(endpoint=pendant_incident, extension=candidate_extension.copy())

        support, probability, satisfies_body, satisfies_full = support_of(g, clause, candidate_extension)
        if support >= min_support:
            if probability <= 0 and probability >= clause.parent.property:
                # either non-existent or no difference
                continue

            # save more constraint clause
            extension = Clause(head=clause.head,
                               body=body,
                               probability=probability,
                               parent=clause)
            extension._satisfy_body = satisfies_body
            extension._satisfy_full = satisfies_full

            extended_clauses.add(extension)

            for extension in extended_clauses:
                extended_clauses |= extend(g, extension, candidate_extension,
                                           {ext for ext in candidate_extensions})

    return extended_clauses

def support_of(g, parent, candidate_extension):
    v, q, w = candidate_extension
    _, s, r = parent.head

    satisfies_body = {constraint for constraint in parent._satisfy_body}
    satisfies_full = {}

    freq = 0
    constraints = shortest_path(source=parent.head,
                               target=candidate_extension,
                               assertions=parent.body)
    for e in parent._satisfy_body:
        if not satisfies(g, e, constraints):
            satisfies_body.discard(e)
            continue

        # check if also satisfies head
        if (e, s, r) in g:
            satisfies_full.add(e)
            freq += 1

    support = len(satisfies_body)

    return (support, freq/support, satisfies_body, satisfies_full)

def satisfies(g, entity, constraints):
    for _, p, o in constraints:
        if (entity, p, o) not in g:
            return False

        entity = o

    return True

def init_generation_forest(g, min_support):
    generation_forest = GenerationForest()

    # gather all types and their members
    class_instance_map = dict()
    for e, _, t in g.triples((None, RDF.type, None)):
        if t not in class_instance_map.keys():
            class_instance_map[t] = set()

        class_instance_map[t].add(e)

    for t in class_instance_map.keys():
        support = len(class_instance_map[t])
        if support < min_support:
            continue

        # gather all predicate-object pairs belonging to the members of a type
        predicate_object_map = dict()
        for e in class_instance_map[t]:
            for _, p, o in g.triples((e, None, None)):
                if p in IGNORE_PREDICATES:
                    continue

                if p not in predicate_object_map.keys():
                    predicate_object_map[p] = dict()
                if o not in predicate_object_map[p].keys():
                    predicate_object_map[p][o] = 0

                predicate_object_map[p][o] = predicate_object_map[p][o] + 1

        # create shared variables
        parent = Clause(head=True, body={})
        var = Clause.ObjectTypeVariable(type=t)

        # generate clauses for each predicate-object pair
        generation_tree = GenerationTree()
        for p in predicate_object_map.keys():
            support = sum(predicate_object_map[p].values())
            if support < min_support:
                continue

            # generate bound objects
            object_types = list()
            object_types_map = dict()
            data_types = list()
            data_types_map = dict()
            for o in predicate_object_map[p].keys():
                # TODO: allow prior background knowledge
                # map resources to types for unbound type generation
                if type(o) is URIRef:
                    ctype = g.value(e, RDF.type)
                    object_types.append(ctype)

                    if ctype not in object_types_map.keys():
                        object_types_map[ctype] = set()
                    object_types_map[ctype].add(o)
                if type(o) is Literal:
                    dtype = o.datatype
                    if dtype is None:
                        dtype = XSD.string if o.language != None else XSD.anyType

                    data_types.append(dtype)
                    if dtype not in data_types_map.keys():
                        data_types_map[dtype] = set()
                    data_types_map[dtype].add(o)

                # TODO: skip if distribution is (close to) uniform
                # create new clause
                phi = Clause(head=Clause.Assertion(var, p, o),
                             body=Clause.Body(identity=Clause.Assertion(var, IDENTITY, var)),
                             probability=predicate_object_map[p][o]/support,
                             parent=parent)

                phi._satisfy_body = class_instance_map[t]
                phi._satisfy_full = {e for e in class_instance_map[t] if (e, p, o) in g}

                generation_tree.add(phi, depth=0)

            # generate unbound object type assertions
            objecttype_count = Counter(object_types)
            for ctype, freq in objecttype_count.items():
                if ctype is None:
                    continue

                var_o = Clause.ObjectTypeVariable(type=ctype)
                phi = Clause(head=Clause.Assertion(var, p, var_o),
                             body=Clause.Body(identity=Clause.Assertion(var, IDENTITY, var)),
                             probability=freq/support,
                             parent=parent)

                phi._satisfy_body = class_instance_map[t]
                phi._satisfy_full = object_types_map[ctype]

                generation_tree.add(phi, depth=0)

            # generate unbound data type assertions
            datatype_count = Counter(data_types)
            for dtype, freq in datatype_count.items():
                if dtype is None:
                    continue

                var_o = Clause.DataTypeVariable(type=dtype)
                phi = Clause(head=Clause.Assertion(var, p, var_o),
                             body=Clause.Body(identity=Clause.Assertion(var, IDENTITY, var)),
                             probability=freq/support,
                             parent=parent)

                phi._satisfy_body = class_instance_map[t]
                phi._satisfy_full = data_types_map[dtype]

                generation_tree.add(phi, depth=0)

        generation_forest.plant(t, generation_tree)

    return generation_forest
