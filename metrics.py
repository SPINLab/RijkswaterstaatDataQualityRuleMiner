#! /usr/bin/env python

from structures import Clause


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
    if not isinstance(assertion.rhs, Clause.TypeVariable):
        # either an entity or literal
        for entity in assertion_domain:
            if assertion.rhs in predicate_map[assertion.predicate]['forwards'][entity]:
                # P(e, u) holds
                assertion_domain_updated.add(entity)
                confidence += 1
    elif isinstance(assertion.rhs, Clause.ObjectTypeVariable):
        for entity in assertion_domain:
            for resource in predicate_map[assertion.predicate]['forwards'][entity]:
                if object_type_map['object-to-type'][resource] is assertion.rhs.type:
                    # P(e, ?) with object type(?, t) holds
                    assertion_domain_updated.add(entity)
                    confidence += 1
    elif isinstance(assertion.rhs, Clause.DataTypeVariable):
        for entity in assertion_domain:
            for resource in predicate_map[assertion.predicate]['forwards'][entity]:
                if data_type_map['object-to-type'][resource] == assertion.rhs.type:
                    # P(e, ?) with data type(?, t) holds
                    assertion_domain_updated.add(entity)
                    confidence += 1

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
    # no need to continue if we are a pendant incident (optimization)
    if len(graph_pattern.connections[assertion]) <= 0:
        if isinstance(assertion, Clause.IdentityAssertion):
            return (len(assertion_domain), assertion_domain)

        support = 0
        assertion_domain_updated = set()
        if not isinstance(assertion.rhs, Clause.TypeVariable):
            # either an entity or literal
            for entity in assertion_domain:
                if assertion.rhs in predicate_map[assertion.predicate]['forwards'][entity]:
                    # P(e, u) holds
                    assertion_domain_updated.add(entity)
                    support += 1
        elif isinstance(assertion.rhs, Clause.ObjectTypeVariable):
            for entity in assertion_domain:
                for resource in predicate_map[assertion.predicate]['forwards'][entity]:
                    if object_type_map['object-to-type'][resource] is assertion.rhs.type:
                        # P(e, ?) with object type(?, t) holds
                        assertion_domain_updated.add(entity)
                        support += 1
        elif isinstance(assertion.rhs, Clause.DataTypeVariable):
            for entity in assertion_domain:
                for resource in predicate_map[assertion.predicate]['forwards'][entity]:
                    if data_type_map['object-to-type'][resource] == assertion.rhs.type:
                        # P(e, ?) with data type(?, t) holds
                        assertion_domain_updated.add(entity)
                        support += 1

        return (support, assertion_domain_updated)

    # retrieve range based on assertion's domain
    if isinstance(assertion, Clause.IdentityAssertion):
        assertion_range = assertion_domain
    else:  # type is Clause.Assertion with ObjectTypeVariable as rhs
        assertion_range = set()
        for entity in assertion_domain:
            for resource in predicate_map[assertion.predicate]['forwards'][entity]:
                if object_type_map['object-to-type'][resource] is assertion.rhs.type:
                    assertion_range.add(resource)

    # update range based on connected assertions' domains (optimization)
    for connection in graph_pattern.connections[assertion]:
        if len(assertion_range) < min_support:
            return (-1, set())

        assertion_range &= frozenset(predicate_map[connection.predicate]['forwards'].keys())

    # update range based on connected assertions' returned updated domains
    # search space is reduced after each returned update
    connection_domain = assertion_range  # only for readability
    for connection in graph_pattern.connections[assertion]:
        if len(assertion_range) < min_support:
            return (-1, set())

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

    # update domain based on updated range
    if isinstance(assertion, Clause.IdentityAssertion):
        return (len(assertion_range), assertion_range)

    support = 0
    assertion_domain_updated = set()
    for resource in assertion_range:
        domain_update = predicate_map[assertion.predicate]['backwards'][resource]
        assertion_domain_updated |= domain_update

        support += len(domain_update)

    return (support, assertion_domain_updated)
