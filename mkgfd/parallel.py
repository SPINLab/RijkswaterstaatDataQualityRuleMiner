#! /usr/bin/env python

from math import ceil
from time import time

from pathos.pools import ProcessPool
from rdflib.namespace import RDF, RDFS, XSD
from rdflib.graph import Literal, URIRef

from mkgfd.structures import (Clause, TypeVariable,
                            DataTypeVariable, MultiModalNode,
                            MultiModalDateFragNode, MultiModalDateTimeNode,
                            MultiModalNumericNode, MultiModalStringNode,
                            ObjectTypeVariable, GenerationForest, GenerationTree)
from mkgfd.cache import Cache
from mkgfd.sequential import (explore, new_clause, new_multimodal_clause,
                        new_variable_clause, map_resources,
                        map_predicate_object_pairs)
from mkgfd.multimodal import (cluster, SUPPORTED_XSD_TYPES, XSD_DATEFRAG,
                        XSD_DATETIME, XSD_NUMERIC, XSD_STRING)




IGNORE_PREDICATES = {RDF.type, RDFS.label}
IDENTITY = URIRef("local://identity")  # reflexive property

def generate_mp(nproc, g, depths, min_support, min_confidence, p_explore, p_extend,
             valprep, prune, mode, max_length_body, max_width, multimodal):
    """ Generate all clauses up to and including a maximum depth which satisfy a minimal
    support and confidence.
    """
    cache = Cache(g)
    with ProcessPool(nproc) as pool:
        t0 = time()
        generation_forest = init_generation_forest_mp(pool, nproc, g, cache.object_type_map,
                                                      min_support, min_confidence,
                                                      mode, multimodal)

        mode_skip_dict = dict()
        npruned = 0
        for depth in range(0, depths.stop):
            print("generating depth {} / {}".format(depth+1, depths.stop))
            for ctype in generation_forest.types():
                print(" type {}".format(ctype), end=" ")

                prune_set = set()
                nclauses = 0
                for phi in generation_forest.get_tree(ctype).get(depth):
                    if depth == 0:
                        if ctype not in mode_skip_dict.keys():
                            mode_skip_dict[ctype] = set()

                        if mode[0] != mode[1] and \
                        (mode[0] == "A" and isinstance(phi.head.rhs, TypeVariable) or
                        mode[0] == "T" and not isinstance(phi.head.rhs, TypeVariable)):
                            # skip clauses with Abox or Tbox heads to filter
                            # exploration on the remainder from depth 0 and 'up'
                            mode_skip_dict[ctype].add(phi)
                            continue

                    elif len(phi.body) >= max_length_body:
                        continue

                    # calculate n without storing the whole set in memory
                    nclauses += 1

                E = set()
                if nclauses >= 1:
                    chunksize = ceil(nclauses/nproc)
                    for psi in pool.uimap(generate_depth_mp,
                                         ((phi,
                                           generate_candidates(phi,
                                                               generation_forest,
                                                               mode,
                                                               depth),
                                           depth,
                                           cache,
                                           prune,
                                           min_support,
                                           min_confidence,
                                           p_explore,
                                           p_extend,
                                           valprep,
                                           mode,
                                           max_length_body,
                                           max_width)
                                          for phi in generation_forest.get_tree(ctype).get(depth)
                                          if phi not in mode_skip_dict[ctype]
                                          and len(phi.body) < max_length_body),
                                         chunksize=chunksize if chunksize > 1 else 2):
                        E.update(psi)

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
                        for derivative in E:
                            if derivative._prune is True:
                                prune_set.add(derivative)

                        E -= prune_set
                        npruned += len(prune_set)

                print("(+{} added)".format(len(E)))

                # remove clauses after generating children if we are
                # not interested in previous depth
                if depth > 0 and depth not in depths:
                    n0 = generation_forest.get_tree(ctype).size
                    generation_forest.clear(ctype, depth)

                    npruned += n0 - generation_forest.get_tree(ctype).size

                generation_forest.update_tree(ctype, E, depth+1)

        if len(mode_skip_dict) > 0:
            # prune unwanted clauses at depth 0 now that we don't need them anymore
            for ctype, skip_set in mode_skip_dict.items():
                generation_forest.prune(ctype, 0, skip_set)

        if 0 not in depths:
            for ctype in generation_forest.types():
                n0 = generation_forest.get_tree(ctype).size
                generation_forest.clear(ctype, 0)

                npruned += n0 - generation_forest.get_tree(ctype).size

        duration = time()-t0
        print('generated {} clauses in {:0.3f}s'.format(
            sum([tree.size for tree in generation_forest._trees.values()]),
            duration),
        end="")

        if npruned > 0:
            print(" ({} pruned)".format(npruned))
        else:
            print()

    return generation_forest

def generate_candidates(phi, generation_forest, mode, depth):
    C = set()
    # only consider unbound object type variables as an extension of
    # a bound entity is already implicitly included
    I = {assertion for assertion in phi.body.distances[depth]
         if type(assertion.rhs) is ObjectTypeVariable}

    for a_i in I:
        if a_i.rhs.type not in generation_forest.types():
            # if the type lacks support, then a clause which uses it will too
            continue

        # gather all possible extensions for an entity of type t
        for psi in generation_forest.get_tree(a_i.rhs.type).get(0):  # J
            a_j = psi.head
            if mode[1] == "A" and isinstance(a_j.rhs, TypeVariable):
                # limit body extensions to Abox
                continue
            if mode[1] == "T" and not isinstance(a_j.rhs, TypeVariable):
                # limit body extensions to Tbox
                continue
            if isinstance(a_j.rhs, MultiModalNode):
                # don't allow multimodal nodes in body
                continue

            C.add((a_i, a_j))

    return C

def generate_depth_mp(inputs):
    phi, C, depth, cache, prune, min_support, min_confidence, \
    p_explore, p_extend, valprep, mode, max_length_body, max_width = inputs

    return explore(phi,
                   C,
                   depth,
                   cache,
                   prune,
                   min_support,
                   min_confidence,
                   p_explore,
                   p_extend,
                   valprep,
                   mode,
                   max_length_body,
                   max_width)


def init_generation_forest_mp(pool, nproc, g, class_instance_map, min_support,
                              min_confidence, mode, multimodal):
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
    for t, tree in pool.uimap(init_generation_tree_mp,
                              ((t,
                                g,
                                class_instance_map,
                                min_support,
                                min_confidence,
                                mode,
                                multimodal) for t in types),
                               chunksize=chunksize if chunksize > 1 else 2):

        offset = len(types)-types.index(t)
        print("\033[F"*offset, end="")
        print(" initialized Generation Tree for type {} (+{} added)".format(str(t),
                                                                            tree.size))
        if offset-1 > 0:
            print("\033[E"*(offset-1), end="")

        if tree.size <= 0:
            continue

        generation_forest.plant(t, tree)

    return generation_forest

def init_generation_tree_mp(inputs):
    t, g, class_instance_map, min_support, min_confidence, mode, multimodal = inputs

    # don't generate what we won't need
    generate_Abox_heads = True
    generate_Tbox_heads = True
    if mode == "AA":
        generate_Tbox_heads = False
    elif mode == "TT":
        generate_Abox_heads = False

    # gather all predicate-object pairs belonging to the members of a type
    predicate_object_map = map_predicate_object_pairs(g, class_instance_map['type-to-object'][t])

    # create shared variables
    parent = Clause(head=True, body={})
    var = ObjectTypeVariable(type=t)

    # generate clauses for each predicate-object pair
    generation_tree = GenerationTree()
    for p in predicate_object_map.keys():
        pfreq = sum(predicate_object_map[p].values())
        if pfreq < min_support:
            # if the number of entities of type t that have this predicate
            # is less than the minimal support, then the overall pattern
            # will have less as well
            continue

        # create clauses for all predicate-object pairs
        object_types_map = dict()
        data_types_map = dict()
        data_types_values_map = dict()
        for o in predicate_object_map[p].keys():
            if generate_Tbox_heads:
                # map resources to types for unbound type generation
                map_resources(g, p, o, class_instance_map['type-to-object'][t],
                              object_types_map, data_types_map)

            if multimodal and type(o) is Literal:
                dtype = o.datatype
                if dtype is None:
                    dtype = XSD.string if o.language != None else XSD.anyType

                if dtype not in SUPPORTED_XSD_TYPES:
                    # skip if not supported
                    continue

                if dtype not in data_types_values_map.keys():
                    data_types_values_map[dtype] = list()
                data_types_values_map[dtype].extend([o]*predicate_object_map[p][o])

        # create clauses for all predicate-object pairs
        for o in predicate_object_map[p].keys():
            if not generate_Abox_heads:
                continue

            #if multimodal and type(o) is Literal:
            #    # skip _all_ literals if we go multimodal
            #    continue

            # create new clause
            phi = new_clause(g, parent, var, p, o,
                             class_instance_map['type-to-object'][t],
                             pfreq, min_confidence)
            if phi is not None:
                generation_tree.add(phi, depth=0)

        # add clauses with variables as objects
        if generate_Tbox_heads:
            # generate unbound object type assertions
            for ctype in object_types_map.keys():
                if ctype is None:
                    continue

                var_o = ObjectTypeVariable(type=ctype)
                phi = new_variable_clause(parent, var, p, var_o,
                                          class_instance_map['type-to-object'][t],
                                          object_types_map[ctype], pfreq, min_confidence)

                if phi is not None:
                    generation_tree.add(phi, depth=0)

            # generate unbound data type assertions
            for dtype in data_types_map.keys():
                if dtype is None:
                    continue

                var_o = DataTypeVariable(type=dtype)
                phi = new_variable_clause(parent, var, p, var_o,
                                          class_instance_map['type-to-object'][t],
                                          data_types_map[dtype], pfreq, min_confidence)

                if phi is not None:
                    generation_tree.add(phi, depth=0)

        # add multimodal nodes
        if multimodal:
            for dtype in data_types_values_map.keys():
                nvalues = len(data_types_values_map[dtype])
                if nvalues < min_confidence:
                    # if the full set does not exceed the threshold then nor
                    # will subsets thereof
                    continue

                # determine clusters per xsd type
                values_sets = cluster(data_types_values_map[dtype],
                                      dtype)
                nsets = len(values_sets)
                if nsets <= 0 or nvalues/nsets < min_confidence:
                    # skip if the theoretical maximum confidence does not
                    # exceed the threshold
                    continue

                nodes = set()
                for value_set in values_sets:
                    if dtype in XSD_NUMERIC:
                        nodes.add(MultiModalNumericNode(dtype,
                                                        *value_set))
                    elif dtype in XSD_DATETIME:
                        nodes.add(MultiModalDateTimeNode(dtype,
                                                         *value_set))
                    elif dtype in XSD_DATEFRAG:
                        nodes.add(MultiModalDateFragNode(dtype,
                                                         *value_set))
                    elif dtype in XSD_STRING:
                        nodes.add(MultiModalStringNode(dtype,
                                                       value_set))

                for node in nodes:
                    phi = new_multimodal_clause(g, parent, var, p, node, dtype,
                                                data_types_values_map,
                                                class_instance_map['type-to-object'][t],
                                                pfreq, min_confidence)

                    if phi is not None:
                        generation_tree.add(phi, depth=0)

    return (t, generation_tree)
