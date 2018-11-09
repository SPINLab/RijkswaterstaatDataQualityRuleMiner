#! /usr/bin/env python

from rdflib.term import Literal, URIRef

from utils import generate_label_map


_CONJUNCTION = "\u2227"
_LEFTARROW = "\u2190"

def prettify(g, clauses):
    results = set()

    entity_label_map = generate_label_map(g)
    for clause in clauses:
        results.add(pretty_clause(clause, entity_label_map))

    return results

def pretty_clause(clause, label_dict):
    head = pretty_assertion(clause.head, label_dict)
    body = " {} ".format(_CONJUNCTION).join(
        [pretty_assertion(assertion, label_dict) for assertion in clause.body])

    return "{:0.3f}: {} {} {{{}}}".format(clause.probability,
                                        head,
                                        _LEFTARROW,
                                        body)

def pretty_assertion(assertion, label_dict):
    # TODO: replace namespaces by prefixes
    assertion_str = "("
    for r in assertion:
        if type(r) is URIRef and r in label_dict.keys():
            assertion_str += str(label_dict[r]) + ", "
        elif type(r) is Literal:
            assertion_str += str(r.value) + ", "
        else:
            assertion_str += str(r) + ", "

    assertion_str = assertion_str[:-2] + ")"

    return assertion_str

