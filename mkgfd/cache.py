#! /usr/bin/env python

from mkgfd.utils import generate_predicate_map, generate_object_type_map, generate_data_type_map


class Cache():
    """ Cache class

    Simple wrapper around several hash maps for fast lookups.
    """
    predicate_map = None
    object_type_map = None
    data_type_map = None

    def __init__(self, g):
        # TODO: compute these more efficient and with less repeation
        self.object_type_map = generate_object_type_map(g)
        self.data_type_map = generate_data_type_map(g)
        self.predicate_map = generate_predicate_map(g)
