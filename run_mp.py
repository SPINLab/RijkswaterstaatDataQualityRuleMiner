#!/usr/bin/env python

import argparse
import csv
import os
import pickle
from time import time
from sys import maxsize

from rdflib import Graph
from rdflib.util import guess_format

from parallel import generate_mp
from ui import _LEFTARROW, _PHI, generate_label_map, pretty_clause
from utils import integerRangeArg


if __name__ == "__main__":
    timestamp = int(time())

    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--nproc", help="Number of cores to utilize",
            default=os.cpu_count())
    parser.add_argument("-d", "--depth", help="Depths to explore",
            type=integerRangeArg, required=True)
    parser.add_argument("-s", "--min_support", help="Minimal clause support",
            required=True)
    parser.add_argument("-c", "--min_confidence", help="Minimal clause confidence",
            required=True)
    parser.add_argument("-o", "--output", help="Preferred output format",
            choices = ["tsv", "pkl"], default="tsv")
    parser.add_argument("-i", "--input", help="One or more RDF-encoded graphs",
            required=True, nargs='+')
    parser.add_argument("--max_size", help="Maximum context size",
            required=False, default=maxsize)
    parser.add_argument("--max_width", help="Maximum width of shell",
            required=False, default=maxsize)
    parser.add_argument("--mode", help="A[box], T[box], or B[oth] as candidates for head and body",
            choices = ["AA", "AT", "TA", "TT", "AB", "BA", "TB", "BT", "BB"], default="BB")
    parser.add_argument("--multimodal", help="Enable multimodal support",
            required=False, action='store_true')
    parser.add_argument("--p_explore", help="Probability of exploring candidate endpoint",
            required=False, default=1.0)
    parser.add_argument("--p_extend", help="Probability of extending at endpoint",
            required=False, default=1.0)
    parser.add_argument("--noprune", help="Do not prune the output set",
            required=False, action='store_true')
    parser.add_argument("--valopt", help="Prepare output for validation (only relevant to pkl)",
            required=False, action='store_true')
    parser.add_argument("--test", help="Dry run without saving results",
            required=False, action='store_true')
    args = parser.parse_args()

    print("nproc: "+str(args.nproc)+"; "+
          "depth: "+str(args.depth)[5:]+"; "+
          "supp: "+str(args.min_support)+"; "+
          "conf: "+str(args.min_confidence)+"; "+
          "p_explore: "+str(args.p_explore)+"; "+
          "p_extend: "+str(args.p_extend)+"; "+
          "mode: "+str(args.mode))

    # load graph(s)
    print("importing graphs...", end=" ")
    g = Graph()
    for gf in args.input:
        g.parse(gf, format=guess_format(gf))
    print("done")

    # only makes sense when using pkl output
    if args.output != "pkl":
        args.valopt = False

    # compute clauses
    f = generate_mp(int(args.nproc), g, args.depth,
                   int(args.min_support), int(args.min_confidence),
                   float(args.p_explore), float(args.p_extend),
                   args.valopt, not args.noprune, args.mode,
                   int(args.max_size), int(args.max_width),
                   args.multimodal)

    if args.test:
        exit(0)

    print("storing results...", end=" ")
    # store clauses
    if args.output == "pkl":
        pickle.dump(f, open("./generation_forest(d{}s{}c{})_{}.pkl".format(str(args.depth)[5:],
                                                                           str(args.min_support),
                                                                           str(args.min_confidence),
                                                                           timestamp), "wb"))
    else:
        ns_dict = {v:k for k,v in g.namespaces()}
        label_dict = generate_label_map(g)
        with open("./generation_forest(d{}s{}c{})_{}.tsv".format(str(args.depth)[5:],
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
