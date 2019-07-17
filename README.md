# Multimodal Knowledge Graph Functional Dependencies (MKGFD)

Mine multimodal functional dependencies from (RDF) knowledge graphs

## MKGFD

A MKGFD `p: head <- body` states that all entities `e` that satisfy the `body` must also satisfy the `head`, with probability `p`.

Here, the `head` consists of a single assertion `P(e, v)`, which states that entity `e` should have element `v` for property `P`. The `body` can take any number of assertions and represents the domain (subgraph) to which the `head` applies. Constraint element `v` can be any entity, an attribute value, a type restriction, or a multimodal cluster. 

## Usage: 

    run_mp.py [-h] 
             [-n NPROC]
             -d DEPTH 
             -s MIN_SUPPORT 
             -c MIN_CONFIDENCE
             -i INPUT [INPUT ...]
             [-o {tsv,pkl}] 
             [--max_size MAX_SIZE]
             [--max_width MAX_WIDTH]
             [--mode {AA,AT,TA,TT,AB,BA,TB,BT,BB}]
             [--multimodal]
             [--p_explore P_EXPLORE]
             [--p_extend P_EXTEND]
             [--noprune]
             [--valopt]
             [--test]


`-n NPROC` - number of virtual processor cores (parallel version only)

`-d DEPTH` - maximum depth body to mine

`-s MIN_SUPPORT` - minimal domain size (entities who satisfy body)

`-c MIN_CONFIDENCE` - minimal range size (entities who satify body and have property `P`

`-i INPUT [INPUT ...]` - input knowledge graph(s)

`-o {tsv,pkl}` - output results for humans (tsv) or tools (pkl)

`--max_size MAX_SIZE` - maximum size of body

`--max_width MAX_WIDTH` - maximum width of body

`--mode {AA,AT,TA,TT,AB,BA,TB,BT,BB}` - (dis)allow Abox and/or Tbox in head and/or body

`--multimodal` - allow for multimodal assertions

`--p_explore P_EXPLORE` - probability of exploring edge

`--p_extend P_EXTEND` - probability of extending body at candidate vertex

`--noprune` - disable pruning

`--valopt` - keep intermediate computations to optimize validation

`--test` - don't write results to disk

## Validation

To validate, use https://gitlab.com/wxwilcke/mkgfdv with generated generation forest (pkl) as input
