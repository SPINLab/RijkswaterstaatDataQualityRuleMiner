#! /usr/bin/env python

from rdflib.namespace import RDF
from rdflib.term import Literal, URIRef

from structures import Clause
from utils import generate_label_map


_CONJUNCTION = "\u2227"
_LEFTARROW = "\u2190"
_PHI = "\u03C6"

def prettify(g, clauses):
    results = set()

    ns_dict = {v:k for k,v in g.namespaces()}
    entity_label_map = generate_label_map(g)
    for clause in clauses:
        results.add(pretty_clause(clause, ns_dict, entity_label_map))

    return results

def pretty_type(clause, ns_dict, label_dict):
    return pretty_assertion((clause.body.identity.lhs,
                             RDF.type,
                             clause.body.identity.lhs.type),
                            ns_dict,
                            label_dict,
                            head=True)

def pretty_clause(clause, ns_dict, label_dict):
    head = pretty_assertion(clause.head, ns_dict, label_dict, head=True)
    body = " {} ".format(_CONJUNCTION).join(
        [pretty_assertion(assertion, ns_dict, label_dict) for assertion in
         clause.body.connections.keys() if not isinstance(assertion, Clause.IdentityAssertion)])

    type = pretty_type(clause, ns_dict, label_dict)
    body = type if len(body) <= 0 else type + " {} ".format(_CONJUNCTION) + body

    return "Pd: {:0.3f}, Pr: {:0.3f}, Supp: {}, Conf: {}\n{}: {} {} {{{}}}".format(
            clause.domain_probability,
            clause.range_probability,
            clause.support,
            clause.confidence,
            _PHI,
            head,
            _LEFTARROW,
            body)

def pretty_assertion(assertion, ns_dict, label_dict, head=False):
    assertion_str = "("
    for r in assertion:
        if type(r) is Literal:
            assertion_str += str(r.value) + ", "
            continue
        if type(r) is URIRef:
            assertion_str += pretty_uri(r, ns_dict)
            if r in label_dict.keys():
                assertion_str += " (" + str(label_dict[r]) + ")"

            assertion_str += ", "
            continue
        if isinstance(r, Clause.TypeVariable):
            if head:
                # assume that the lhs variable is always inspected first
                assertion_str += "[SELF], "
                head = False
                continue

            assertion_str += pretty_uri(r.type, ns_dict)
            assertion_str += " [ObjectType Variable]" if isinstance(r, Clause.ObjectTypeVariable)\
                                                     else " [DataType Variable]"
            assertion_str += ", "
            continue

        assertion_str += str(r) + ", "  # fallback

    assertion_str = assertion_str[:-2] + ")"

    return assertion_str

def pretty_uri(uri, ns_dict):
    for ns in ns_dict.keys():
        if ns not in uri:
            continue

        qname = str(uri)[len(ns):]
        if len(qname) <= 0:
            continue

        return "{}:{}".format(ns_dict[ns], qname)

    return str(uri)
