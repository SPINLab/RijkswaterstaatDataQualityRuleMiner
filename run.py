#!/usr/bin/env python

import argparse
import csv
import pickle
from sys import exit
from time import time

from rdflib import Graph
from rdflib.util import guess_format

from sequential import generate
from ui import _LEFTARROW, _PHI, generate_label_map, pretty_clause


if __name__ == "__main__":
    timestamp = int(time())

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--max_depth", help="Maximum depth to explore",
            required=True)
    parser.add_argument("-s", "--min_support", help="Minimal clause support",
            required=True)
    parser.add_argument("-c", "--min_confidence", help="Minimal clause confidence",
            required=True)
    parser.add_argument("-o", "--output", help="Preferred output format",
            choices = ["tsv", "pkl"], default="tsv")
    parser.add_argument("-i", "--input", help="One or more RDF-encoded graphs",
            required=True, nargs='+')
    parser.add_argument("--p_explore", help="Probability of exploring candidate endpoint",
            required=False, default=1.0)
    parser.add_argument("--p_extend", help="Probability of extending at endpoint",
            required=False, default=1.0)
    parser.add_argument("--valopt", help="Prepare output for validation (only relevant to pkl)",
            required=False, action='store_true')
    parser.add_argument("--test", help="Dry run without saving results",
            required=False, action='store_true')
    args = parser.parse_args()

    print("depth: "+str(args.max_depth)+"; "+
          "supp: "+str(args.min_support)+"; "+
          "conf: "+str(args.min_confidence)+"; "+
          "p_explore: "+str(args.p_explore)+"; "+
          "p_extend: "+str(args.p_extend))


    # load graph(s)
    print("importing graphs...", end=" ")
    g = Graph()
    for gf in args.input:
        g.parse(gf, format=guess_format(gf))
    print("done")

    # only makes sense when using pkl output
    if args.output != "pkl":
        args.valopt = False

    # compute clause
    f = generate(g, int(args.max_depth),
                 int(args.min_support), int(args.min_confidence),
                 float(args.p_explore), float(args.p_extend),
                 args.valopt)

    if args.test:
        exit(0)

    print("storing results...", end=" ")
    # store clauses
    if args.output == "pkl":
        pickle.dump(f, open("./generation_forest(d{}s{}c{})_{}.pkl".format(str(args.max_depth),
                                                                           str(args.min_support),
                                                                           str(args.min_confidence),
                                                                           timestamp), "wb"))
    else:
        ns_dict = {v:k for k,v in g.namespaces()}
        label_dict = generate_label_map(g)
        with open("./generation_forest(d{}s{}c{})_{}.tsv".format(str(args.max_depth),
                                                                 str(args.min_support),
                                                                 str(args.min_confidence),
                                                                 timestamp), "w") as ofile:
            writer = csv.writer(ofile, delimiter="\t")
            writer.writerow(['Depth', 'P_domain', 'P_range', 'Supp', 'Conf', 'Head', 'Body'])
            for c in f.get():
                depth = max(c.body.distances.keys())
                bare = pretty_clause(c, ns_dict, label_dict).split("\n"+_PHI+": ")[-1].split(" "+_LEFTARROW+" ")
                writer.writerow([depth,
                                 c.domain_probability, c.range_probability,
                                 c.support, c.confidence,
                                 bare[0], bare[1]])

    print("done")
