#! /usr/bin/env python

from collections import Counter
from math import ceil
from multiprocessing import Pool
from time import process_time

from rdflib.namespace import RDF, RDFS, XSD
from rdflib.graph import Literal, URIRef

from structures import Assertion, Clause, ClauseBody, TypeVariable, DataTypeVariable, IdentityAssertion, ObjectTypeVariable, GenerationForest, GenerationTree
from cache import Cache
from sequential import explore


IGNORE_PREDICATES = {RDF.type, RDFS.label}
IDENTITY = URIRef("local://identity")  # reflexive property

def generate_mp(nproc, g, depths, min_support, min_confidence, p_explore,
                p_extend, valprep, prune, mode):
    """ Generate all clauses up to and including a maximum depth which satisfy a minimal
    support and confidence.

    Parallel computation support
    """
    cache = Cache(g)
    with Pool(nproc) as pool:
        generation_forest = init_generation_forest_mp(pool, nproc, g, cache.object_type_map,
                                                   min_support, min_confidence, mode)

        mode_skip_dict = dict()
        npruned = 0
        t0 = process_time()
        for depth in range(0, depths.stop):
            print("generating depth {} / {}".format(depth+1, depths.stop))
            for ctype in generation_forest.types():
                print(" type {}".format(ctype), end=" ")

                prune_set = set()
                nclauses = 0
                for clause in generation_forest.get_tree(ctype).get(depth):
                    # calculate n without storing the whole set in memory
                    nclauses += 1

                    if depth == 0:
                        if ctype not in mode_skip_dict.keys():
                            mode_skip_dict[ctype] = set()

                        if mode[0] != mode[1] and \
                        (mode[0] == "A" and isinstance(clause.head.rhs, TypeVariable) or
                        mode[0] == "T" and not isinstance(clause.head.rhs, TypeVariable)):
                            # skip clauses with Abox or Tbox heads to filter
                            # exploration on the remainder from depth 0 and 'up'
                            mode_skip_dict[ctype].add(clause)

                derivatives = set()
                if nclauses >= 1:
                    chunksize = ceil(nclauses/nproc)
                    for clause_derivatives in pool.imap_unordered(generate_depth_mp,
                                                                 ((clause,
                                                                   g,
                                                                   generation_forest,
                                                                   depth,
                                                                   cache,
                                                                   min_support,
                                                                   min_confidence,
                                                                   p_explore,
                                                                   p_extend,
                                                                   valprep,
                                                                   mode)
                                                                  for clause in generation_forest.get_tree(ctype).get(depth)
                                                                  if clause not in mode_skip_dict[ctype]),
                                                                 chunksize=chunksize if chunksize > 1 else 2):
                        derivatives.update(clause_derivatives)

                for clause in generation_forest.get_tree(ctype).get(depth):
                    # clear domain of clause (which we won't need anymore) to save memory
                    clause._satisfy_body = None
                    clause._satisfy_full = None

                    if prune and depth > 0 and clause._prune is True:
                        prune_set.add(clause)

                # prune clauses after generating children to still allow for complex children
                if prune:
                    generation_forest.prune(ctype, depth, prune_set)
                    npruned += len(prune_set)

                    # prune children in last iteration
                    if depth == depths.stop-1:
                        prune_set = set()
                        for derivative in derivatives:
                            if derivative._prune is True:
                                prune_set.add(derivative)

                        derivatives -= prune_set
                        npruned += len(prune_set)

                print("(+{} added)".format(len(derivatives)))

                # remove clauses after generating children if we are
                # not interested in previous depth
                if depth > 0 and depth not in depths:
                    n0 = generation_forest.get_tree(ctype).size
                    generation_forest.clear(ctype, depth)

                    npruned += n0 - generation_forest.get_tree(ctype).size

                generation_forest.update_tree(ctype, derivatives, depth+1)

        if len(mode_skip_dict) > 0:
            # prune unwanted clauses at depth 0 now that we don't need them anymore
            for ctype, skip_set in mode_skip_dict.items():
                generation_forest.prune(ctype, 0, skip_set)

        if 0 not in depths:
            for ctype in generation_forest.types():
                n0 = generation_forest.get_tree(ctype).size
                generation_forest.clear(ctype, 0)

                npruned += n0 - generation_forest.get_tree(ctype).size

        duration = process_time()-t0
        print('generated {} clauses in {:0.3f}s'.format(
            sum([tree.size for tree in generation_forest._trees.values()]),
            duration),
        end="")

        if npruned > 0:
            print(" ({} pruned)".format(npruned))
        else:
            print()

    return generation_forest

def generate_depth_mp(inputs):
    clause, g, generation_forest, depth, cache, min_support, min_confidence, \
    p_explore, p_extend, valprep, mode = inputs
    pendant_incidents = {assertion for assertion in clause.body.distances[depth]
                            if type(assertion.rhs) is ObjectTypeVariable}

    return explore(g,
                   generation_forest,
                   clause,
                   pendant_incidents,
                   depth,
                   cache,
                   min_support,
                   min_confidence,
                   p_explore,
                   p_extend,
                   valprep,
                   mode)


def init_generation_forest_mp(pool, nproc, g, class_instance_map, min_support,
                              min_confidence, mode):
    """ Initialize the generation forest by creating all generation trees of
    types which satisfy minimal support and confidence.
    """
    print("initializing Generation Forest")
    generation_forest = GenerationForest()

    types = list()
    for t in class_instance_map['type-to-object'].keys():
        # if the number of type instances do not exceed the minimal support then
        # any pattern of this type will not either
        support = len(class_instance_map['type-to-object'][t])
        if support >= min_support:
            print(" initializing Generation Tree for type {}...".format(str(t)))
            types.append(t)

    chunksize = ceil(len(types)/nproc)
    for t, tree in pool.imap_unordered(init_generation_tree_mp,
                                      ((t,
                                        g,
                                        class_instance_map,
                                        min_support,
                                        min_confidence,
                                        mode) for t in types),
                                       chunksize=chunksize if chunksize > 1 else 2):

        offset = len(types)-types.index(t)
        print("\033[F"*offset, end="")
        print(" initialized Generation Tree for type {} (+{} added)".format(str(t),
                                                                            tree.size))
        if offset-1 > 0:
            print("\033[E"*(offset-1), end="")

        generation_forest.plant(t, tree)

    return generation_forest

def init_generation_tree_mp(inputs):
    t, g, class_instance_map, min_support, min_confidence, mode = inputs

    # don't generate what we won't need
    generate_Abox_heads = True
    generate_Tbox_heads = True
    if mode == "AA":
        generate_Tbox_heads = False
    elif mode == "TT":
        generate_Abox_heads = False

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
            if generate_Tbox_heads:
                # map resources to types for unbound type generation
                if type(o) is URIRef:
                    ctype = g.value(o, RDF.type)
                    if ctype is None:
                        ctype = RDFS.Class
                    object_types.append(ctype)

                    if ctype not in object_types_map.keys():
                        object_types_map[ctype] = set()
                    object_types_map[ctype].update(
                        {e for e in class_instance_map['type-to-object'][t] if (e, p, o) in g})
                if type(o) is Literal:
                    dtype = o.datatype
                    if dtype is None:
                        dtype = XSD.string if o.language != None else XSD.anyType

                    data_types.append(dtype)
                    if dtype not in data_types_map.keys():
                        data_types_map[dtype] = set()
                    data_types_map[dtype].update(
                        {e for e in class_instance_map['type-to-object'][t] if (e, p, o) in g})

            if not generate_Abox_heads:
                continue

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

        if generate_Tbox_heads:
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
