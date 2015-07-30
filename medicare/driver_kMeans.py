# -*- coding: utf-8 -*-
"""
Driver to run k-means

@author: steven
"""

import argparse
import os
import sys
input_hdfs='assignment3/medicare/standardized_Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt'
output_hdfs='assignment3/medicareOut'
centroids_hdfs='assignment3/medicareCentroids/centroids.txt'
maxIter = 10
nClusters = 5
random = ' -r'

# remove output folder in hadoop
os.system("python kMeans.py " + input_hdfs + " " + output_hdfs + " " + centroids_hdfs + " -maxIter " + str(maxIter) + " -nClusters " + str(nClusters) + random)