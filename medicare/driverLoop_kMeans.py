# -*- coding: utf-8 -*-
"""
Driver to run k-means
Iterate maxIter, number of clusters and number of initial random centroids

@author: steven
"""

import argparse
import os
import sys

input_hdfs='assignment3/medicare/standardized_Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt'
output_hdfs='assignment3/medicareOut'
maxIter = 2
random = ' -r'

minCluster = 3
maxCluster = 3
maxRandom = 1

# iterate over clusterSize
for n in range(minCluster,maxCluster+1):
	# iteratve over different random points
	for i in range(1,maxRandom+1):
		centroids_hdfs='assignment3/medicareCentroids/centroids'+ str(n) + '-' + str(i) + '.txt'
		nClusters = n
		os.system("python kMeans.py " + input_hdfs + " " + output_hdfs + " " + centroids_hdfs + " -maxIter " + str(maxIter) + " -nClusters " + str(nClusters) + random)