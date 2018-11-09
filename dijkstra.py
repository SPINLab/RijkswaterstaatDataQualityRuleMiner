#! /usr/bin/env python

from math import inf


def shortest_path(source, target, assertions):
    # Dijkstra's shortest path

    target = Edge(target)
    edges = {Edge(assertion) for assertion in assertions}

    # distance to self is 0
    edges.add(Edge(source, 0))

    while len(edges) > 0:
        # select nearest edge
        edge, d = None, inf
        for candidate_edge in edges:
            if candidate_edge.distance < d:
                d = candidate_edge.distance
                edge = candidate_edge
        edges.discard(edge)

        # goal reached
        if edge.assertion[2] == target.assertion[0]:  # edge endpoint matches
            path = [target.assertion]
            while edge is not None:
                path.append(edge.assertion)
                edge = edge.parent

            path.reverse()
            return path

        # explore connected edges
        for candidate_neighbour in edges:
            if edge.assertion[2] == candidate_neighbour.assertion[0]:  # edge endpoint matches
                d = edge.distance + 1
                if d < candidate_neighbour.distance:
                    candidate_neighbour.distance = d
                    candidate_neighbour.parent = edge


class Edge():
    distance = -1
    parent = None
    assertion = None

    def __init__(self, assertion, distance=inf):
        self.assertion = assertion
        self.distance = distance
        self.parent = None

    def __str__(self):
        return "[{}] {} [{}]".format(self.distance, self.assertion, self.parent)

    def __repr__(self):
        return "Edge [{}] {}".format(self.distance, self.assertion)
