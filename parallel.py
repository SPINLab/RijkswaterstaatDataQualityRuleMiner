#! /usr/bin/env python

from collections import Counter
from math import ceil
from multiprocessing import Pool
import logging

from rdflib.namespace import RDF, RDFS, XSD
from rdflib.graph import Literal, URIRef

from structures import Assertion, Clause, ClauseBody, DataTypeVariable, IdentityAssertion, ObjectTypeVariable, GenerationForest, GenerationTree
from cache import Cache
from sequential import explore


IGNORE_PREDICATES = {RDF.type, RDFS.label}
IDENTITY = URIRef("local://identity")  # reflexive property

log = logging.getLogger(__name__)

def generate_mp(nproc, g, max_depth, min_support, min_confidence):
    """ Generate all clauses up to and including a maximum depth which satisfy a minimal
    support and confidence.

    Parallel computation support
    """
    cache = Cache(g)
    with Pool(nproc) as pool:
        generation_forest = init_generation_forest_mp(pool, g, cache.object_type_map,
                                                   min_support, min_confidence)

        for depth in range(0, max_depth):
            log.debug("Generating depth {} / {}".format(depth+1, max_depth))
            print("Generating depth {} / {}".format(depth+1, max_depth))
            for ctype in generation_forest.types():
                print(" Type {}".format(ctype))

                clauses = set(generation_forest.get(ctype, depth))
                derivatives = set()
                for clause_derivatives in pool.imap_unordered(generate_depth_mp,
                                                             ((clause,
                                                               g,
                                                               generation_forest,
                                                               depth,
                                                               cache,
                                                               min_support,
                                                               min_confidence) for
                                                               clause in clauses),
                                                              chunksize=ceil(len(clauses)/nproc)):
                    derivatives.update(clause_derivatives)

                generation_forest.update_tree(ctype, derivatives, depth+1)

                log.debug("Adding {} clauses to depth {} of tree {}".format(len(derivatives),
                                                                            depth+1,
                                                                            str(ctype)))

    return generation_forest

def generate_depth_mp(inputs):
    clause, g, generation_forest, depth, cache, min_support, min_confidence = inputs
    pendant_incidents = {assertion for assertion in clause.body.distances[depth]
                            if type(assertion.rhs) is ObjectTypeVariable}

    return explore(g,
                   generation_forest,
                   clause,
                   pendant_incidents,
                   depth,
                   cache,
                   min_support,
                   min_confidence)


def init_generation_forest_mp(pool, g, class_instance_map, min_support, min_confidence):
    """ Initialize the generation forest by creating all generation trees of
    types which satisfy minimal support and confidence.
    """
    print("Initializing Generation Forest")
    log.debug("Initializing Generation Forest")
    generation_forest = GenerationForest()

    types = set()
    for t in class_instance_map['type-to-object'].keys():
        # if the number of type instances do not exceed the minimal support then
        # any pattern of this type will not either
        support = len(class_instance_map['type-to-object'][t])
        if support >= min_support:
            types.add(t)

    trees = pool.map(init_generation_tree_mp,
                    ((t,
                      g, 
                      class_instance_map,
                      min_support, 
                      min_confidence)
                     for t in types))

    for t, tree in trees:
        generation_forest.plant(t, tree)

    return generation_forest

def init_generation_tree_mp(inputs):
    t, g, class_instance_map, min_support, min_confidence = inputs

    log.debug(" Initializing Generation Tree for type {}".format(str(t)))
    print("Initializing Generation Tree for type {}".format(str(t)))
    # gather all predicate-object pairs belonging to the members of a type
    predicate_object_map = dict()
    for e in class_instance_map['type-to-object'][t]:
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
    var = ObjectTypeVariable(type=t)

    # generate clauses for each predicate-object pair
    generation_tree = GenerationTree()
    for p in predicate_object_map.keys():
        pfreq = sum(predicate_object_map[p].values())
        if pfreq < min_support:
            continue

        # create clauses for all predicate-object pairs
        object_types = list()
        object_types_map = dict()
        data_types = list()
        data_types_map = dict()
        for o in predicate_object_map[p].keys():
            # map resources to types for unbound type generation
            if type(o) is URIRef:
                ctype = g.value(o, RDF.type)
                if ctype is None:
                    ctype = RDFS.Class
                object_types.append(ctype)

                if ctype not in object_types_map.keys():
                    object_types_map[ctype] = list()
                object_types_map[ctype].append(o)
            if type(o) is Literal:
                dtype = o.datatype
                if dtype is None:
                    dtype = XSD.string if o.language != None else XSD.anyType

                data_types.append(dtype)
                if dtype not in data_types_map.keys():
                    data_types_map[dtype] = list()
                data_types_map[dtype].append(o)

            # create new clause
            phi = Clause(head=Assertion(var, p, o),
                         body=ClauseBody(identity=IdentityAssertion(var, IDENTITY, var)),
                         parent=parent)

            phi._satisfy_body = {e for e in class_instance_map['type-to-object'][t]}
            phi._satisfy_full = {e for e in class_instance_map['type-to-object'][t] if (e, p, o) in g}

            phi.support = len(phi._satisfy_body)
            phi.confidence = len(phi._satisfy_full)
            phi.domain_probability = phi.confidence/phi.support
            phi.range_probability = phi.confidence/pfreq

            if phi.confidence >= min_confidence:
                generation_tree.add(phi, depth=0)

        # generate unbound object type assertions
        objecttype_count = Counter(object_types)
        for ctype, ofreq in objecttype_count.items():
            if ctype is None:
                continue

            var_o = ObjectTypeVariable(type=ctype)
            phi = Clause(head=Assertion(var, p, var_o),
                         body=ClauseBody(identity=IdentityAssertion(var, IDENTITY, var)),
                         parent=parent)

            phi._satisfy_body = {e for e in class_instance_map['type-to-object'][t]}
            phi._satisfy_full = {e for e in object_types_map[ctype]}

            phi.support = len(phi._satisfy_body)
            phi.confidence = len(phi._satisfy_full)
            phi.domain_probability = phi.confidence/phi.support
            phi.range_probability = phi.confidence/pfreq

            if phi.confidence >= min_confidence:
                generation_tree.add(phi, depth=0)

        # generate unbound data type assertions
        datatype_count = Counter(data_types)
        for dtype, ofreq in datatype_count.items():
            if dtype is None:
                continue

            var_o = DataTypeVariable(type=dtype)
            phi = Clause(head=Assertion(var, p, var_o),
                         body=ClauseBody(identity=IdentityAssertion(var, IDENTITY, var)),
                         parent=parent)

            phi._satisfy_body = {e for e in class_instance_map['type-to-object'][t]}
            phi._satisfy_full = {e for e in data_types_map[dtype]}

            phi.support = len(phi._satisfy_body)
            phi.confidence = len(phi._satisfy_full)
            phi.domain_probability = phi.confidence/phi.support
            phi.range_probability = phi.confidence/pfreq

            if phi.confidence >= min_confidence:
                generation_tree.add(phi, depth=0)

    return (t, generation_tree)
