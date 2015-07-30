# -*- coding: utf-8 -*-
"""
Driver to run k-means
Iterate maxIter, number of clusters and number of initial random centroids

@author: steven
"""

import argparse
import os
import sys

input_hdfs='assignment3/medicare/standardized2_Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt'
output_hdfs='assignment3/medicareOut'
centroids_hdfs='assignment3/medicareCentroids/centroids.txt'

delim = '/'
inputPathList = input_hdfs.split(delim)
inputFileName = inputPathList[-1]
inputFilePath = delim.join(inputPathList[0:-1])
newFileName = "cluster_" + inputFileName

# remove output folder in hadoop
os.system("hadoop fs -rm -r " + output_hdfs)
# run map reduce job
os.system("hadoop jar lin_rehman_exercise2.jar " + input_hdfs + " " + output_hdfs + " " + centroids_hdfs)
# combine outputs to local
os.system("hadoop fs -getmerge " + output_hdfs + " " + newFileName )
# remove crc file http://stackoverflow.com/questions/15434709/checksum-exception-when-reading-from-or-copying-to-hdfs-in-apache-hadoop
os.system("rm " + "." + newFileName + ".crc")
# remove hadoop folder
os.system("hadoop fs -rm -r " + output_hdfs)
    
print ("......Output File ({0}) located in current directory".format(newFileName))