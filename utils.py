#! /usr/bin/env python

from rdflib.namespace import RDF, RDFS


def generate_label_map(g):
    label_map = dict()
    for e, _, l in g.triples((None, RDFS.label, None)):
        label_map[e] = l

    return label_map

def generate_type_map(g):
    type_map = dict()
    for e, _, t in g.triples((None, RDF.type, None)):
        type_map[e] = t

    return type_map
