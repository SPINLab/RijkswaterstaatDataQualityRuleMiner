# Multimodal Knowledge Graph Functional Dependencies (MKGFD)

Mine multimodal functional dependencies from (RDF) knowledge graphs

## MKGFD

A MKGFD `p: head <- body` states that all entities `e` that satisfy the `body` must also satisfy the `head`, with probability `p`.

Here, the `head` consists of a single assertion `P(e, v)`, which states that entity `e` should have element `v` for property `P`. The `body` can take any number of assertions and represents the domain (subgraph) to which the `head` applies. Constraint element `v` can be any entity, an attribute value, a type restriction, or a multimodal cluster. 

## Usage: 

    usage: run_mp.py [-h] [-n NPROC] -d DEPTH -s MIN_SUPPORT -c MIN_CONFIDENCE
                     [-o {tsv,pkl}] -i INPUT [INPUT ...] [--max_size MAX_SIZE]
                     [--max_width MAX_WIDTH] [--mode {AA,AT,TA,TT,AB,BA,TB,BT,BB}]
                     [--multimodal] [--p_explore P_EXPLORE] [--p_extend P_EXTEND]
                     [--noprune] [--valopt] [--test]

    optional arguments:
      -h, --help            show this help message and exit
      -n NPROC, --nproc NPROC
                            Number of cores to utilize
      -d DEPTH, --depth DEPTH
                            Depths to explore
      -s MIN_SUPPORT, --min_support MIN_SUPPORT
                            Minimal clause support
      -c MIN_CONFIDENCE, --min_confidence MIN_CONFIDENCE
                            Minimal clause confidence
      -o {tsv,pkl}, --output {tsv,pkl}
                            Preferred output format
      -i INPUT [INPUT ...], --input INPUT [INPUT ...]
                            One or more RDF-encoded graphs
      --max_size MAX_SIZE   Maximum context size
      --max_width MAX_WIDTH
                            Maximum width of shell
      --mode {AA,AT,TA,TT,AB,BA,TB,BT,BB}
                            A[box], T[box], or B[oth] as candidates for head and
                            body
      --multimodal          Enable multimodal support
      --p_explore P_EXPLORE
                            Probability of exploring candidate endpoint
      --p_extend P_EXTEND   Probability of extending at endpoint
      --noprune             Do not prune the output set
      --valopt              Prepare output for validation (only relevant to pkl)
      --test                Dry run without saving results

## Validation

To validate, use https://gitlab.com/wxwilcke/mkgfdv with generated generation forest (pkl) as input
