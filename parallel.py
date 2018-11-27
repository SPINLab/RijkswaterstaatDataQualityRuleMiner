#! /usr/bin/env python

from collections import Counter
from multiprocessing import Manager, Process
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
    """
    cache = Cache(g)
    generation_forest = init_generation_forest_mp(nproc, g, cache.object_type_map,
                                               min_support, min_confidence)

    for depth in range(0, max_depth):
        log.debug("Generating depth {} / {}".format(depth+1, max_depth))
        for ctype in generation_forest.types():
            with Manager() as manager:
                jobs = manager.Queue()
                derivatives = manager.list()

                # populate queue
                for clause in generation_forest.get(ctype, depth):
                   jobs.put(clause)

                pool = set()
                for proc in range(nproc):
                    pool.add(Process(target=generate_depth_mp, args=(jobs,
                                                                  derivatives,
                                                                  g,
                                                                  generation_forest,
                                                                  depth,
                                                                  cache,
                                                                  min_support,
                                                                  min_confidence)))

                for p in pool:
                    p.start()

                for p in pool:
                    # need to wait because depth d+1 relies on depth d
                    p.join()

                generation_forest.update_tree(ctype, set(derivatives), depth+1)

                log.debug("Adding {} clauses to depth {} of tree {}".format(len(derivatives),
                                                                            depth+1,
                                                                            str(ctype)))

    return generation_forest

def generate_depth_mp(jobs, results, g, generation_forest, depth, cache,
                   min_support, min_confidence):
    while not jobs.empty():
        clause = jobs.get()

        pendant_incidents = {assertion for assertion in clause.body.distances[depth]
                                if type(assertion.rhs) is ObjectTypeVariable}

        results.extend(list(explore(g,
                                   generation_forest,
                                   clause,
                                   pendant_incidents,
                                   depth,
                                   cache,
                                   min_support,
                                   min_confidence)))

def init_generation_forest_mp(nproc, g, class_instance_map, min_support, min_confidence):
    """ Initialize the generation forest by creating all generation trees of
    types which satisfy minimal support and confidence.
    """
    log.debug("Initializing Generation Forest")
    generation_forest = GenerationForest()

    with Manager() as manager:
        jobs = manager.Queue()
        results = manager.list()

        for t in class_instance_map['type-to-object'].keys():
            # if the number of type instances do not exceed the minimal support then
            # any pattern of this type will not either
            support = len(class_instance_map['type-to-object'][t])
            if support >= min_support:
                jobs.put(t)

        pool = set()
        for proc in range(nproc):
            pool.add(Process(target=init_generation_tree_mp, args=(jobs, results,
                                                                g, class_instance_map,
                                                                min_support, min_confidence)))

        for p in pool:
            p.start()

        for p in pool:
            p.join()

        for t, tree in results:
            generation_forest.plant(t, tree)

    return generation_forest

def init_generation_tree_mp(jobs, results, g, class_instance_map, min_support, min_confidence):
    while not jobs.empty():
        t = jobs.get()

        log.debug("Initializing Generation Tree for type {}".format(str(t)))
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

        results.append((t, generation_tree))
