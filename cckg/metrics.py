#! /usr/bin/env python

from mkgfd.structures import IdentityAssertion, DataTypeVariable, MultiModalNode, ObjectTypeVariable, TypeVariable
from mkgfd.utils import cast_xsd


def confidence_of(predicate_map,
               object_type_map,
               data_type_map,
               assertion,
               assertion_domain):
    """ Calculate confidence for a Clause head

    Assumes that domain satisfies the Clause body that belongs to this head
    """
    confidence = 0
    assertion_domain_updated = set()
    if not isinstance(assertion.rhs, TypeVariable):
        # either an entity or literal
        for entity in assertion_domain:
            if assertion.rhs in predicate_map[assertion.predicate]['forwards'][entity]:
                # P(e, u) holds
                assertion_domain_updated.add(entity)
                confidence += 1
    elif isinstance(assertion.rhs, ObjectTypeVariable):
        for entity in assertion_domain:
            satisfied = False
            for resource in predicate_map[assertion.predicate]['forwards'][entity]:
                for ctype in object_type_map['object-to-type'][resource]:
                    if ctype == assertion.rhs.type:
                        # P(e, ?) with object type(?, t) holds
                        assertion_domain_updated.add(entity)
                        confidence += 1
                        satisfied = True
                        break

                if satisfied:
                    break
    elif isinstance(assertion.rhs, DataTypeVariable):
        for entity in assertion_domain:
            for resource in predicate_map[assertion.predicate]['forwards'][entity]:
                if data_type_map['object-to-type'][resource] == assertion.rhs.type:
                    # P(e, ?) with data type(?, t) holds
                    assertion_domain_updated.add(entity)
                    confidence += 1
                    break
    elif isinstance(assertion.rhs, MultiModalNode):
        for entity in assertion_domain:
            for resource in predicate_map[assertion.predicate]['forwards'][entity]:
                if data_type_map['object-to-type'][resource] == assertion.rhs.type\
                   and cast_xsd(resource, assertion.rhs.type) in assertion.rhs:
                    # P(e, u) with u satisfied by multimodal pattern
                    assertion_domain_updated.add(entity)
                    confidence +=1
                    break

    return (confidence, assertion_domain_updated)

def support_of(predicate_map,
            object_type_map,
            data_type_map,
            graph_pattern,
            assertion,
            assertion_domain,
            min_support):
    """ Calculate Minimal Image-Based Support for a Clause body

    Returns -1 if support < min_support

    Optimized to minimalize the work done by continuously reducing the search
    space and by early stopping when possible
    """

    assertion_key = hash(assertion)
    # no need to continue if we are a leaf (optimization)
    if len(graph_pattern.connections[assertion_key]) <= 0:
        if isinstance(assertion, IdentityAssertion):
            return (len(assertion_domain), {e for e in assertion_domain})

        support = 0
        assertion_domain_updated = set()
        if not isinstance(assertion.rhs, TypeVariable):
            # either an entity or literal
            for entity in assertion_domain:
                if assertion.rhs in predicate_map[assertion.predicate]['forwards'][entity]:
                    # P(e, u) holds
                    assertion_domain_updated.add(entity)
                    support += 1
        elif isinstance(assertion.rhs, ObjectTypeVariable):
            for entity in assertion_domain:
                satisfied = False
                for resource in predicate_map[assertion.predicate]['forwards'][entity]:
                    for ctype in object_type_map['object-to-type'][resource]:
                        if ctype == assertion.rhs.type:
                            # P(e, ?) with object type(?, t) holds
                            assertion_domain_updated.add(entity)
                            support += 1
                            satisfied = True
                            break

                if satisfied:
                    break
        elif isinstance(assertion.rhs, DataTypeVariable):
            for entity in assertion_domain:
                for resource in predicate_map[assertion.predicate]['forwards'][entity]:
                    if data_type_map['object-to-type'][resource] == assertion.rhs.type:
                        # P(e, ?) with data type(?, t) holds
                        assertion_domain_updated.add(entity)
                        support += 1
                        break
        elif isinstance(assertion.rhs, MultiModalNode):
            for entity in assertion_domain:
                for resource in predicate_map[assertion.predicate]['forwards'][entity]:
                    if data_type_map['object-to-type'][resource] == assertion.rhs.type\
                       and cast_xsd(resource, assertion.rhs.type) in assertion.rhs:
                        # P(e, u) with u satisfied by multimodal pattern
                        assertion_domain_updated.add(entity)
                        support +=1
                        break

        return (support, assertion_domain_updated)

    # retrieve range based on assertion's domain
    if isinstance(assertion, IdentityAssertion):
        assertion_range = {e for e in assertion_domain}
    else:  # type is Assertion with ObjectTypeVariable as rhs
        assertion_range = set()
        for entity in assertion_domain:
            for resource in predicate_map[assertion.predicate]['forwards'][entity]:
                for ctype in object_type_map['object-to-type'][resource]:
                    if ctype == assertion.rhs.type:
                        # Note: type check requires '==' as ID changes in MP mode
                        assertion_range.add(resource)
                        break

    # update range by intersections with domains of connected assertions (optimization)
    # eg, if p(e, v) and q(.,.), check if e in domain of q
    for connection in graph_pattern.connections[assertion_key]:
        assertion_range &= frozenset(predicate_map[connection.predicate]['forwards'].keys())

        if len(assertion_range) < min_support:
            return (-1, set())

    # update range based on connected assertions' returned updated domains
    # search space is reduced after each returned update
    connection_domain = assertion_range  # only for readability
    for connection in graph_pattern.connections[assertion_key]:
        support, range_update = support_of(predicate_map,
                                        object_type_map,
                                        data_type_map,
                                        graph_pattern,
                                        connection,
                                        connection_domain,
                                        min_support)
        if support < min_support:
            return (-1, set())

        assertion_range &= range_update

        if len(assertion_range) < min_support:
            return (-1, set())

    # update domain based on updated range
    if isinstance(assertion, IdentityAssertion):
        return (len(assertion_range), assertion_range)

    assertion_domain_updated = set()
    for resource in assertion_range:
        domain_update = predicate_map[assertion.predicate]['backwards'][resource]
        assertion_domain_updated |= domain_update

    support = len(assertion_domain_updated)

    return (support, assertion_domain_updated)
