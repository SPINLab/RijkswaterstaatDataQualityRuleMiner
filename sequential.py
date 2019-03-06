#! /usr/bin/env python

from collections import Counter
from random import random

from rdflib.namespace import RDF, RDFS, XSD
from rdflib.graph import Literal, URIRef

from structures import Assertion, Clause, ClauseBody, DataTypeVariable, IdentityAssertion, ObjectTypeVariable, GenerationForest, GenerationTree
from cache import Cache
from metrics import support_of, confidence_of
from utils import isEquivalent, predicate_frequency


IGNORE_PREDICATES = {RDF.type, RDFS.label}
IDENTITY = URIRef("local://identity")  # reflexive property

def generate(g, max_depth, min_support, min_confidence, p_explore, p_extend):
    """ Generate all clauses up to and including a maximum depth which satisfy a minimal
    support and confidence.
    """
    cache = Cache(g)
    generation_forest = init_generation_forest(g, cache.object_type_map,
                                               min_support, min_confidence)

    for depth in range(0, max_depth):
        print("Generating depth {} / {}".format(depth+1, max_depth))
        for ctype in generation_forest.types():
            print(" Type {}".format(ctype), end=" ")
            derivatives = set()

            for clause in generation_forest.get_tree(ctype).get(depth):
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
                                       p_extend)

            print("(+{} added)".format(len(derivatives)))
            generation_forest.update_tree(ctype, derivatives, depth+1)

    return generation_forest

def explore(g, generation_forest,
            clause, pendant_incidents,
            depth, cache, min_support,
            min_confidence, p_explore,
            p_extend):
    """ Explore all predicate-object pairs which where added by the previous
    iteration as possible endpoints to expand from.
    """
    extended_clauses = set()

    while len(pendant_incidents) > 0:
        pendant_incident = pendant_incidents.pop()

        if pendant_incident.rhs.type not in generation_forest.types():
            # if the type lacks support, then a clause which uses it will too
            continue

        # skip with probability of (1.0 - p_explore)
        if p_explore < random():
            continue

        # gather all possible extensions for an entity of type t
        candidate_extensions = {candidate_clause.head for candidate_clause in
                                generation_forest.get_tree(pendant_incident.rhs.type).get(0)}

        # evaluate all candidate extensions for this depth
        extensions = extend(g, clause, pendant_incident, candidate_extensions,
                            cache, depth, min_support, min_confidence, p_extend)
        extended_clauses |= extensions

        for extended_clause in extensions:
            extended_clauses |= explore(g, generation_forest, extended_clause,
                                        pendant_incidents, depth, cache,
                                        min_support, min_confidence, p_explore,
                                        p_extend)

    return extended_clauses

def extend(g, parent, pendant_incident, candidate_extensions, cache,
           depth, min_support, min_confidence, p_extend):
    """ Extend a clause from a given endpoint variable by evaluating all
    possible candidate extensions on whether they satisfy the minimal support
    and confidence.
    """
    extended_clauses = set()

    while len(candidate_extensions) > 0:
        candidate_extension = candidate_extensions.pop()

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
        if support >= min_support:
            # compute confidence
            confidence, satisfies_full = confidence_of(cache.predicate_map,
                                                       cache.object_type_map,
                                                       cache.data_type_map,
                                                       head,
                                                       satisfies_body)
            if confidence < min_confidence:
                continue

            # skip with probability of (1 - p_extend)
            # place it here as we only want to skip those we are really adding
            if p_extend < random():
                continue

            # save more constraint clause
            extented_clause = Clause(head=head,
                                     body=body,
                                     parent=parent)
            extented_clause._satisfy_body = satisfies_body
            extented_clause._satisfy_full = satisfies_full

            extented_clause.support = support
            extented_clause.confidence = confidence
            extented_clause.domain_probability = confidence / support

            pfreq = predicate_frequency(cache.predicate_map,
                                        head,
                                        satisfies_body)
            extented_clause.range_probability = confidence / pfreq

            # add link for validation optimization
            parent.children.add(extented_clause)

            # save new clause
            extended_clauses.add(extented_clause)

            # expand new clause on same depth
            extended_clauses |= extend(g,
                                       extented_clause,
                                       pendant_incident,
                                       {ext for ext in candidate_extensions},
                                       cache,
                                       depth,
                                       min_support,
                                       min_confidence,
                                       p_extend)

    return extended_clauses

def init_generation_forest(g, class_instance_map, min_support, min_confidence):
    """ Initialize the generation forest by creating all generation trees of
    types which satisfy minimal support and confidence.
    """
    print("Initializing Generation Forest")
    generation_forest = GenerationForest()

    for t in class_instance_map['type-to-object'].keys():
        # if the number of type instances do not exceed the minimal support then
        # any pattern of this type will not either
        support = len(class_instance_map['type-to-object'][t])
        if support < min_support:
            continue

        print(" Initializing Generation Tree for type {}...".format(str(t)), end=" ")
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
            if pfreq < min_confidence:
                # if the number of entities of type t that have this predicate
                # is less than the minimal confidence, then the overall pattern
                # will have less as well
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

                # create new clause
                phi = Clause(head=Assertion(var, p, o),
                             body=ClauseBody(identity=IdentityAssertion(var, IDENTITY, var)),
                             parent=parent)

                phi._satisfy_body = {e for e in class_instance_map['type-to-object'][t]}
                phi._satisfy_full = {e for e in phi._satisfy_body if (e, p, o) in g}

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

        if generation_tree.size <= 0:
            continue

        print("done (+{} added)".format(generation_tree.size))
        generation_forest.plant(t, generation_tree)

    return generation_forest
