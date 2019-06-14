#! /usr/bin/env python

from datetime import datetime

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
NORMALIZED_DELTA_MIN = 0.1

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

    return clusters

def numeric_clusters(X):
    """ X := a 1D numpy array """

    if len(X.shape) != 2:
        X = X.reshape(-1, 1)  # one-dimensional vector

    distortions = []
    models = []
    for k in range(CLUSTERS_MIN, CLUSTERS_MAX+1):
        model = KMeans(n_clusters=k).fit(X)
        models.append(model)

        distortions.append(sum(np.min(cdist(X, model.cluster_centers_, 'euclidean'), axis=1)) / X.shape[0])

    # determine optimal k using elbow method
    deltas = [distortions[i]-distortions[i+1] for i in range(len(distortions)-1)]

    dmin = min(deltas)
    dmax = max(deltas)
    deltas_normalized = [(delta-dmin)/(dmax-dmin) for delta in deltas]

    for i in range(len(deltas_normalized)):
        if deltas_normalized[i] < NORMALIZED_DELTA_MIN:
            # optimal k = i
            break

    return [(float(round(cc[0]-distortions[i-1], 3)),
             float(round(cc[0]+distortions[i-1], 3)))
            for cc in models[i-1].cluster_centers_]
