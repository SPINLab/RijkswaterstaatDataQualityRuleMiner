#! /usr/bin/env python

from collections import Counter
from datetime import datetime
import warnings

from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist
import numpy as np
from rdflib.namespace import XSD

from timeutils import gFrag_to_days


XSD_DATEFRAG = {XSD.gDay, XSD.gMonth, XSD.gMonthDay, XSD.gYear, XSD.gYearMonth}
XSD_DATETIME = {XSD.date, XSD.dateTime, XSD.dateTimeStamp}
XSD_NUMERIC = {XSD.integer, XSD.nonNegativeInteger, XSD.positiveInteger,
               XSD.float, XSD.decimal, XSD.double, XSD.negativeInteger,
               XSD.nonPositiveInteger}
XSD_STRING = {XSD.string, XSD.normalizedString}
SUPPORTED_XSD_TYPES = set().union(XSD_DATEFRAG,
                                  XSD_DATETIME,
                                  XSD_NUMERIC,
                                  XSD_STRING)

CLUSTERS_MIN = 1
CLUSTERS_MAX = 10
NORMALIZED_MIN = 0.1

def cluster(object_list, dtype):
    clusters = []
    if dtype in XSD_NUMERIC:
        X = np.array([float(v) for v in object_list])

        clusters = numeric_clusters(X)
    elif dtype in XSD_DATETIME:
        # cluster on POSIX timestamps
        X = np.array([datetime.fromisoformat(v).timestamp() for
                      v in object_list])

        clusters = [(datetime.fromtimestamp(begin),
                     datetime.fromtimestamp(end)) for begin, end in numeric_clusters(X)]
    elif dtype in XSD_DATEFRAG:
        # cluster on days
        X = np.array([gFrag_to_days(v, dtype) for v in object_list])

        clusters = numeric_clusters(X)
    elif dtype in XSD_STRING:
        clusters = string_clusters(object_list)

    return clusters

def numeric_clusters(X, acc=3):
    """ X := a 1D numpy array """

    if len(X.shape) != 2:
        X = X.reshape(-1, 1)  # one-dimensional vector

    distortions = []
    models = []
    for k in range(CLUSTERS_MIN, CLUSTERS_MAX+1):
        if len(X) < k:
            break

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model = KMeans(n_clusters=k).fit(X)
            models.append(model)

        distortions.append(sum(np.min(cdist(X, model.cluster_centers_, 'euclidean'), axis=1)) / X.shape[0])

    # determine optimal k using elbow method
    deltas = [distortions[i]-distortions[i+1] for i in range(len(distortions)-1)]

    dmin = min(deltas)
    dmax = max(deltas)
    deltas_normalized = [(delta-dmin)/(dmax-dmin) for delta in deltas
                         if (dmax-dmin) > 0 else 0.0]

    for i in range(len(deltas_normalized)):
        if deltas_normalized[i] < NORMALIZED_MIN:
            # optimal k = i
            break

    return [(float(round(cc[0]-distortions[i-1], acc)),
             float(round(cc[0]+distortions[i-1], acc)))
            for cc in models[i-1].cluster_centers_]

def string_clusters(object_list, strict=True):
    regex_patterns = list()
    for s in object_list:
        regex_patterns.append(generate_regex(s))

    if not strict:
        return list(generalize_regex(regex_patterns))

    weighted = {k:v**2 for k,v in Counter(regex_patterns).items()}

    wmin = min(weighted.values())
    wmax = max(weighted.values())
    if wmin == wmax:
        return regex_patterns

    weighted_normalized = {k:(v-wmin)/(wmax-wmin) for k,v in weighted.items()}

    return [pattern for pattern in weighted_normalized.keys() if
            weighted_normalized[pattern] >= NORMALIZED_MIN]

def generalize_regex(patterns):
    generalized_patterns = set()

    subpattern_list = list()
    for pattern in patterns:
        if len(pattern) <= 2:
            # empty string
            continue

        subpatterns = pattern[1:-1].split('\s')
        if subpatterns[-1][:-3].endswith('[(\.|\?|!)]'):
            end = subpatterns[-1][-14:]
            subpatterns[-1] = subpatterns[-1][:-14]
            subpatterns.append(end)

        for i, subpattern in enumerate(subpatterns):
            if len(subpattern_list) <= i:
                subpattern_list.append(dict())

            char_pattern = subpattern[:-3]
            if char_pattern not in subpattern_list[i].keys():
                subpattern_list[i][char_pattern] = list()
            subpattern_list[i][char_pattern].append(int(subpattern[-2:-1]))

    subpattern_cluster_list = list()
    for i, subpatterns in enumerate(subpattern_list):
        if len(subpattern_cluster_list) <= i:
            subpattern_cluster_list.append(dict())

        for subpattern, lengths in subpatterns.items():
            if subpattern not in subpattern_cluster_list[i].keys():
                subpattern_cluster_list[i][subpattern] = list()

            if len(lengths) <= 2 or len(set(lengths)) == 1:
                clusters = [(min(lengths), max(lengths))]
            else:
                clusters = [(int(a), int(b)) for a,b in
                            numeric_clusters(np.array(lengths), acc=0)]

            subpattern_cluster_list[i][subpattern] = clusters

    for pattern in patterns:
        subpatterns = pattern[1:-1].split('\s')
        if subpatterns[-1][:-3].endswith('[(\.|\?|!)]'):
            end = subpatterns[-1][-14:]
            subpatterns[-1] = subpatterns[-1][:-14]
            subpatterns.append(end)
        generalized_patterns |= combine_regex(subpatterns,
                                              subpattern_cluster_list)

    return generalized_patterns

def combine_regex(subpatterns, subpattern_cluster_list, _pattern='', _i=0):
    if len(subpatterns) <= 0:
        return {_pattern+'$'}

    patterns = set()
    char_pattern = subpatterns[0][:-3]
    if char_pattern in subpattern_cluster_list[_i].keys():
        for a,b in subpattern_cluster_list[_i][char_pattern]:
            if a == b:
                length = '{' + str(a) + '}'
            else:
                length = '{' + str(a) + ',' + str(b) + '}'

            if _i <= 0:
                pattern = '^' + char_pattern + length
            elif char_pattern == "[(\.|\?|!)]":
                pattern = _pattern + char_pattern + length
            else:
                pattern = _pattern + '\s' + char_pattern + length

            patterns |= combine_regex(subpatterns[1:], subpattern_cluster_list,
                                      pattern, _i+1)

    return patterns

def generate_regex(s):
    s = ' '.join(s.split())

    pattern = '^'
    if len(s) <= 0:
        # empty string
        return pattern + '$'

    prev_char_class = character_class(s[0])
    count = 0
    for i in range(len(s)):
        char_class = character_class(s[i])

        if char_class == prev_char_class:
            count += 1

            if i < len(s)-1:
                continue

        pattern += prev_char_class
        if prev_char_class != "\s":
            pattern += '{' + str(count) + '}'
        count = 1
        if i >= len(s)-1 and char_class != prev_char_class:
            pattern += char_class
            if char_class != "\s":
                pattern += '{' + str(count) + '}'

        prev_char_class = char_class

    return pattern + '$'

def character_class(c):
    if c.isalpha():
        char_class = "[a-z]" if c.islower() else "[A-Z]"
    elif c.isdigit():
        char_class = "\d"
    elif c.isspace():
        char_class = "\s"
    elif c == "." or c == "?" or c == "!":
        char_class = "[(\.|\?|!)]"
    else:
        char_class = "[^A-Za-z0-9\.\?! ]"

    return char_class
