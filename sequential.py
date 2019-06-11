#! /usr/bin/env python

from collections import Counter
from random import random
from time import process_time

from rdflib.namespace import RDF, RDFS, XSD
from rdflib.graph import Literal, URIRef

from structures import Assertion, Clause, ClauseBody, TypeVariable, DataTypeVariable, IdentityAssertion, ObjectTypeVariable, GenerationForest, GenerationTree
from cache import Cache
from metrics import support_of, confidence_of
from utils import isEquivalent, predicate_frequency


IGNORE_PREDICATES = {RDF.type, RDFS.label}
IDENTITY = URIRef("local://identity")  # reflexive property

def generate(g, depths, min_support, min_confidence, p_explore, p_extend,
             valprep, prune, mode, max_length_body, max_width):
    """ Generate all clauses up to and including a maximum depth which satisfy a minimal
    support and confidence.
    """
    cache = Cache(g)
    generation_forest = init_generation_forest(g, cache.object_type_map,
                                               min_support, min_confidence,
                                               mode)

    mode_skip_dict = dict()
    npruned = 0
    t0 = process_time()
    for depth in range(0, depths.stop):
        print("generating depth {} / {}".format(depth+1, depths.stop))
        for ctype in generation_forest.types():
            print(" type {}".format(ctype), end=" ")
            derivatives = set()
            prune_set = set()

            for clause in generation_forest.get_tree(ctype).get(depth):
                if depth == 0 and mode[0] != mode[1] and \
                   (mode[0] == "A" and isinstance(clause.head.rhs, TypeVariable) or
                    mode[0] == "T" and not isinstance(clause.head.rhs, TypeVariable)):
                    # skip clauses with Abox or Tbox heads to filter
                    # exploration on the remainder from depth 0 and 'up'
                    if ctype not in mode_skip_dict.keys():
                        mode_skip_dict[ctype] = set()
                    mode_skip_dict[ctype].add(clause)

                    continue

                if len(clause.body) < max_length_body:
                    # only consider unbound object type variables as an extension of
                    # a bound entity is already implicitly included
                    pendant_incidents = {assertion for assertion in clause.body.distances[depth]
                                            if type(assertion.rhs) is ObjectTypeVariable}

                    derivatives |= explore(g,
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
                                           mode,
                                           max_length_body,
                                           max_width)

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

def explore(g, generation_forest,
            clause, pendant_incidents,
            depth, cache, min_support,
            min_confidence, p_explore,
            p_extend, valprep, mode,
            max_length_body, max_width):
    """ Explore all predicate-object pairs which where added by the previous
    iteration as possible endpoints to expand from.
    """
    extended_clauses = set()
    clause_incident_map = dict()
    unsupported_incidents = set()
    for pendant_incident in pendant_incidents:
        if pendant_incident.rhs.type not in generation_forest.types():
            # if the type lacks support, then a clause which uses it will too
            continue

        # skip with probability of (1.0 - p_explore)
        if p_explore < random():
            continue

        # gather all possible extensions for an entity of type t
        candidate_extensions = set()
        for candidate_clause in generation_forest.get_tree(pendant_incident.rhs.type).get(0):
            candidate_extension = candidate_clause.head
            if mode[1] == "A" and isinstance(candidate_extension.rhs, TypeVariable):
                # limit body extensions to Abox
                continue
            if mode[1] == "T" and not isinstance(candidate_extension.rhs, TypeVariable):
                # limit body extensions to Tbox
                continue

            candidate_extensions.add(candidate_extension)

        # evaluate all candidate extensions for this depth
        extensions = extend(g, clause, pendant_incident, candidate_extensions,
                            cache, depth, min_support, min_confidence, p_extend,
                            valprep, max_length_body, max_width, 0)

        if len(extensions) <= 0:
            unsupported_incidents.add(pendant_incident)
            continue

        extended_clauses |= extensions
        for extended_clause in extended_clauses:
            # remember which incident was explored (optimization)
            clause_incident_map[extended_clause] = pendant_incident

    # prune step (future recursions will not explore these)
    pendant_incidents -= unsupported_incidents

    for extended_clause in {ext for ext in extended_clauses}:
        # rmv corresponding extension to avoid duplicates in recursions
        pendant_incidents.discard(clause_incident_map[extended_clause])

        if len(extended_clause.body) >= max_length_body:
            continue

        extended_clauses |= explore(g, generation_forest, extended_clause,
                                    {pi for pi in pendant_incidents}, depth, cache,
                                    min_support, min_confidence, p_explore,
                                    p_extend, valprep, mode, max_length_body,
                                    max_width)

    return extended_clauses

def extend(g, parent, pendant_incident, candidate_extensions, cache,
           depth, min_support, min_confidence, p_extend, valprep,
           max_length_body, max_width, _width):
    """ Extend a clause from a given endpoint variable by evaluating all
    possible candidate extensions on whether they satisfy the minimal support
    and confidence.
    """
    extended_clauses = set()
    clause_extension_map = dict()
    unsupported_extensions = set()

    if _width >= max_width:
        # the class constraint (depth '0') is excluded
        return extended_clauses

    for candidate_extension in candidate_extensions:

        # omit if candidate for level 0 is equivalent to head
        if depth == 0 and isEquivalent(parent.head, candidate_extension, cache):
            continue

        # omit equivalents on same context level (exact or by type)
        if depth+1 in parent.body.distances.keys():
            equivalent = False
            for assertion in parent.body.distances[depth+1]:
                if isEquivalent(assertion, candidate_extension, cache):
                    equivalent = True
                    break

            if equivalent:
                continue

        # create new clause body by extending that of the parent
        head = parent.head
        body = parent.body.copy()
        body.extend(endpoint=pendant_incident, extension=candidate_extension.copy())

        # compute support
        support, satisfies_body = support_of(cache.predicate_map,
                                             cache.object_type_map,
                                             cache.data_type_map,
                                             body,
                                             body.identity,
                                             {e for e in parent._satisfy_body},
                                             min_support)

        if support < min_support:
            unsupported_extensions.add(candidate_extension)
            continue

        # compute confidence
        confidence, satisfies_full = confidence_of(cache.predicate_map,
                                                   cache.object_type_map,
                                                   cache.data_type_map,
                                                   head,
                                                   satisfies_body)
        if confidence < min_confidence:
            unsupported_extensions.add(candidate_extension)
            continue

        # skip with probability of (1 - p_extend)
        # place it here as we only want to skip those we are really adding
        if p_extend < random():
            continue

        # save more constraint clause
        extended_clause = Clause(head=head,
                                 body=body,
                                 parent=parent)
        extended_clause._satisfy_body = satisfies_body
        extended_clause._satisfy_full = satisfies_full

        extended_clause.support = support
        extended_clause.confidence = confidence
        extended_clause.domain_probability = confidence / support

        pfreq = predicate_frequency(cache.predicate_map,
                                    head,
                                    satisfies_body)
        extended_clause.range_probability = confidence / pfreq

        # set delayed pruning if no reduction in domain
        # NOTE: (BUG) this isn't deterministic when recursively extending
        if support >= parent.support:
            extended_clause._prune = True

        # remember which extension was added (optimization)
        clause_extension_map[extended_clause] = candidate_extension

        # add link for validation optimization
        if valprep:
            parent.children.add(extended_clause)

        # save new clause
        extended_clauses.add(extended_clause)

    # pruning step (future recursions will not evaluate these)
    candidate_extensions -= unsupported_extensions

    for extended_clause in {extcl for extcl in extended_clauses}:
        # rmv corresponding extension to avoid duplicates in recursions
        candidate_extensions.discard(clause_extension_map[extended_clause])

        if len(extended_clause.body) >= max_length_body:
            continue

        # expand new clause on same depth
        extended_clauses |= extend(g,
                                   extended_clause,
                                   pendant_incident,
                                   {cext for cext in candidate_extensions},
                                   cache,
                                   depth,
                                   min_support,
                                   min_confidence,
                                   p_extend,
                                   valprep,
                                   max_length_body,
                                   max_width,
                                   _width+1)

    return extended_clauses

def init_generation_forest(g, class_instance_map, min_support, min_confidence,
                           mode):
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
                # if the number of entities of type t that have this predicate
                # is less than the minimal support, then the overall pattern
                # will have less as well
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

        print("done (+{} added)".format(generation_tree.size))

        if generation_tree.size <= 0:
            continue

        generation_forest.plant(t, generation_tree)

    return generation_forest
