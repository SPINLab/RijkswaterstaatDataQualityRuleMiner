#! /usr/bin/env python

from random import random, choice
from time import time
from multiprocessing import Manager

from rdflib.namespace import RDF, RDFS, XSD
from rdflib.graph import Literal, URIRef

from mkgfd.structures import (Assertion, Clause, ClauseBody, TypeVariable,
                            DataTypeVariable, IdentityAssertion,
                            MultiModalNode,
                            MultiModalDateFragNode, MultiModalDateTimeNode,
                            MultiModalNumericNode, MultiModalStringNode,
                            ObjectTypeVariable, GenerationForest, GenerationTree)
from mkgfd.cache import Cache
from mkgfd.metrics import support_of, confidence_of
from mkgfd.multimodal import (cluster, SUPPORTED_XSD_TYPES, XSD_DATEFRAG,
                        XSD_DATETIME, XSD_NUMERIC, XSD_STRING)
from mkgfd.utils import cast_xsd, isEquivalent, predicate_frequency


IGNORE_PREDICATES = {RDF.type, RDFS.label}
IDENTITY = URIRef("local://identity")  # reflexive property

def generate(g, depths, min_support, min_confidence, p_explore, p_extend,
             valprep, prune, mode, max_length_body, max_width, multimodal):
    """ Generate all clauses up to and including a maximum depth which satisfy a minimal
    support and confidence.
    """
    cache = Cache(g)

    t0 = time()
    generation_forest = init_generation_forest(g, cache.object_type_map,
                                               min_support, min_confidence,
                                               mode, multimodal)

    del g  # save memory

    mode_skip_dict = dict()
    npruned = 0
    for depth in range(0, depths.stop):
        print("generating depth {} / {}".format(depth+1, depths.stop))
        for ctype in generation_forest.types():
            print(" type {}".format(ctype), end=" ")
            E = set()
            prune_set = set()

            for phi in generation_forest.get_tree(ctype).get(depth):
                #if depth == 0 and prune and\
                #   (isinstance(clause.head.rhs, ObjectTypeVariable) or
                #    isinstance(clause.head.rhs, DataTypeVariable)):
                #    # assume that predicate range is consistent irrespective of
                #    # context beyond depth 0
                #    npruned += 1

                #    continue

                if depth == 0 and mode[0] != mode[1] and \
                   (mode[0] == "A" and isinstance(phi.head.rhs, TypeVariable) or
                    mode[0] == "T" and not isinstance(phi.head.rhs, TypeVariable)):
                    # skip clauses with Abox or Tbox heads to filter
                    # exploration on the remainder from depth 0 and 'up'
                    if ctype not in mode_skip_dict.keys():
                        mode_skip_dict[ctype] = set()
                    mode_skip_dict[ctype].add(phi)

                    continue

                if len(phi.body) < max_length_body:
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

                    E |= explore(phi,
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

                # clear domain of clause (which we won't need anymore) to save memory
                phi._satisfy_body = None
                phi._satisfy_full = None

                if prune and depth > 0 and phi._prune is True:
                    prune_set.add(phi)

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

    if 0 not in depths and depths.stop-depths.start > 0:
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

def visited(V, body, a_i, a_j):
    body.extend(endpoint=a_i, extension=a_j)
    return body in V

def covers(body, a_i, a_j):
    return a_j in body.connections[hash(a_i)]

def bad_combo(U, body, a_i, a_j):
    body.extend(endpoint=a_i, extension=a_j)
    return body in U

def explore(phi, C,
            depth, cache, prune, min_support,
            min_confidence, p_explore,
            p_extend, valprep, mode,
            max_length_body, max_width):
    """ Explore all predicate-object pairs which where added by the previous
    iteration as possible endpoints to expand from.
    """
    E = set()  # extended clauses
    V = set()  # visited clauses
    U = set()  # bad combinations

    with Manager() as manager:
        qexplore = manager.Queue()
        qexplore.put(phi)
        while not qexplore.empty():
            psi = qexplore.get()

            if len(psi.body) == max_length_body:
                continue

            if depth+1 in psi.body.distances.keys():
                if len(psi.body.distances[depth+1]) >= max_width:
                    continue

            # skip with probability of (1 - p_explore)
            skip_endpoint = None
            if p_explore < random():
                skip_endpoint = choice(tuple(C))

            for a_i, a_j in C:
                # test identity here as endpoint is same object
                if a_j is skip_endpoint:
                    continue

                # skip with probability of (1 - p_extend)
                # place it here as we only want to skip those we are really adding
                if p_extend < random():
                    continue

                if visited(V, psi.body.copy(), a_i, a_j)\
                   or covers(psi.body, a_i, a_j)\
                   or bad_combo(U, psi.body.copy(), a_i, a_j):
                    continue

                chi = extend(psi, a_i, a_j, cache, depth,
                             min_support, min_confidence)

                if chi is not None:
                    qexplore.put(chi)
                    E.add(chi)
                    V.add(chi.body.copy())

                    # add link for validation optimization
                    if valprep:
                        psi.children.add(chi)
                else:
                    U.add((psi.body.copy(), a_i, a_j))

        if len(E) <= 0 or not prune:
            return E

        # set delayed pruning on siblings if all have same support/confidence
        # (ie, as it doesn't matter which extension we add, we can assume that none really matter)
        qprune = manager.Queue()
        qprune.put(phi)
        while not qprune.empty():
            psi = qprune.get()
            scores_set = list()
            for chi in psi.children:
                scores_set.append((chi.support, chi.confidence))

                if len(chi.children) > 0:
                    qprune.put(chi)

            if len(psi.children) >= 2\
               and scores_set.count(scores_set)[0] == len(scores_set):
                for chi in psi.children:
                    chi._prune = True

    return E

def extend(psi, a_i, a_j, cache,
           depth, min_support, min_confidence):
    """ Extend a clause from a given endpoint variable by evaluating all
    possible candidate extensions on whether they satisfy the minimal support
    and confidence.
    """

    # omit if candidate for level 0 is equivalent to head
    if depth == 0 and isEquivalent(psi.head, a_j, cache):
        return None

    # omit equivalents on same context level (exact or by type)
    if depth+1 in psi.body.distances.keys():
        equivalent = False
        for assertion in psi.body.distances[depth+1]:
            if isEquivalent(assertion, a_j, cache):
                equivalent = True
                break

        if equivalent:
            return None

    # create new clause body by extending that of the parent
    head = psi.head
    body = psi.body.copy()
    body.extend(endpoint=a_i, extension=a_j)

    # compute support
    support, satisfies_body = support_of(cache.predicate_map,
                                         cache.object_type_map,
                                         cache.data_type_map,
                                         body,
                                         body.identity,
                                         psi._satisfy_body,
                                         min_support)

    if support < min_support:
        return None

    # compute confidence
    confidence, satisfies_full = confidence_of(cache.predicate_map,
                                               cache.object_type_map,
                                               cache.data_type_map,
                                               head,
                                               satisfies_body)
    if confidence < min_confidence:
        return None

    # save more constraint clause
    chi = Clause(head=head,
                 body=body,
                 parent=psi)
    chi._satisfy_body = satisfies_body
    chi._satisfy_full = satisfies_full

    chi.support = support
    chi.confidence = confidence
    chi.domain_probability = confidence / support

    pfreq = predicate_frequency(cache.predicate_map,
                                head,
                                satisfies_body)
    chi.range_probability = confidence / pfreq

    # set delayed pruning if no reduction in domain
    if support >= psi.support:
        chi._prune = True

    return chi

def init_generation_forest(g, class_instance_map, min_support, min_confidence,
                           mode, multimodal):
    """ Initialize the generation forest by creating all generation trees of
    types which satisfy minimal support and confidence.
    """
    print("initializing Generation Forest")
    generation_forest = GenerationForest()

    # don't generate what we won't need
    generate_Abox_heads = True
    generate_Tbox_heads = True
    if mode == "AA":
        generate_Tbox_heads = False
    elif mode == "TT":
        generate_Abox_heads = False

    for t in class_instance_map['type-to-object'].keys():
        # if the number of type instances do not exceed the minimal support then
        # any pattern of this type will not either
        support = len(class_instance_map['type-to-object'][t])
        if support < min_support:
            continue

        print(" initializing Generation Tree for type {}...".format(str(t)), end=" ")
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

        print("done (+{} added)".format(generation_tree.size))

        if generation_tree.size <= 0:
            continue

        generation_forest.plant(t, generation_tree)

    return generation_forest

def new_clause(g, parent, var, p, o, class_instance_map, pfreq, min_confidence):
    phi = Clause(head=Assertion(var, p, o),
                 body=ClauseBody(identity=IdentityAssertion(var, IDENTITY, var)),
                 parent=parent)

    phi._satisfy_full = {e for e in class_instance_map if (e, p, o) in g}
    phi.confidence = len(phi._satisfy_full)

    if phi.confidence < min_confidence:
        return None

    phi._satisfy_body = {e for e in class_instance_map}
    phi.support = len(phi._satisfy_body)

    phi.domain_probability = phi.confidence/phi.support
    phi.range_probability = phi.confidence/pfreq

    return phi

def new_variable_clause(parent, var, p, o, class_instance_map, types_map, pfreq, min_confidence):
    phi = Clause(head=Assertion(var, p, o),
                 body=ClauseBody(identity=IdentityAssertion(var, IDENTITY, var)),
                 parent=parent)

    phi._satisfy_full = {e for e in types_map}
    phi.confidence = len(phi._satisfy_full)
    if phi.confidence < min_confidence:
        return None

    phi._satisfy_body = {e for e in class_instance_map}
    phi.support = len(phi._satisfy_body)

    phi.domain_probability = phi.confidence/phi.support
    phi.range_probability = phi.confidence/pfreq

    return phi

def new_multimodal_clause(g, parent, var, p, node, dtype, data_types_values_map,
                          class_instance_map, pfreq, min_confidence):
    phi = Clause(head=Assertion(var, p, node),
                 body=ClauseBody(identity=IdentityAssertion(var, IDENTITY, var)),
                 parent=parent)

    phi._satisfy_full = set()
    for e in class_instance_map:
        for o in g.objects(e, p):
            if type(o) is Literal and\
               o in data_types_values_map[dtype] and\
               cast_xsd(o, dtype) in node:
                phi._satisfy_full.add(e)

    phi.confidence = len(phi._satisfy_full)
    if phi.confidence < min_confidence:
        return None

    phi._satisfy_body = {e for e in class_instance_map}

    phi.support = len(phi._satisfy_body)
    phi.domain_probability = phi.confidence/phi.support
    phi.range_probability = phi.confidence/pfreq

    return phi

# map rhs (data)type to lhs entities
def map_resources(g, p, o, class_instance_map,
                  object_types_map, data_types_map):
    types = list()
    if type(o) is URIRef:
        types = list(g.objects(o, RDF.type))
        if len(types) <= 0:
            types.append(RDFS.Class)

        types_map = object_types_map
    elif type(o) is Literal:
        t = o.datatype
        if t is None:
            t = XSD.string if o.language != None else XSD.anyType
        types.append(t)

        types_map = data_types_map
    else:
        return

    for t in types:
        if t not in types_map.keys():
            types_map[t] = set()
        types_map[t].update({e for e in class_instance_map if (e, p, o) in g})

# map and count every (p ,o)-pair belonging to entities of this type
def map_predicate_object_pairs(g, class_instance_map):
    predicate_object_map = dict()
    for e in class_instance_map:
        for _, p, o in g.triples((e, None, None)):
            if p in IGNORE_PREDICATES:
                continue

            if p not in predicate_object_map.keys():
                predicate_object_map[p] = dict()
            if o not in predicate_object_map[p].keys():
                predicate_object_map[p][o] = 0

            predicate_object_map[p][o] = predicate_object_map[p][o] + 1

    return predicate_object_map
