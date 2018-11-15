#! /usr/bin/env python

from utils import generate_predicate_map, generate_object_type_map, generate_data_type_map


class Cache():
    predicate_map = None
    object_type_map = None
    data_type_map = None

    def __init__(self, g, min_support):
        self.object_type_map = generate_object_type_map(g, min_support)
        self.data_type_map = generate_data_type_map(g)
        self.predicate_map = generate_predicate_map(g, min_support)
