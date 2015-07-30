# -*- coding: utf-8 -*-
"""
Python Script to run K-means Map Reduce

@author: steven
"""
# https://docs.python.org/2/howto/argparse.html
# https://docs.python.org/3/library/argparse.html
# https://mkaz.com/2014/07/26/python-argparse-cookbook/
# http://pymotw.com/2/argparse/

# import os
# import sys

# input_hdfs = sys.argv[1]
# out_hdfs = sys.argv[2]
# num_cluster=int(raw_input('Number of cluster:'))

import argparse
import os
import sys
parser = argparse.ArgumentParser(description='*** K-Means ***')
parser.add_argument("input_hdfs", help="Input folder [or file] path in Hadoop [e.g. input OR input/data.txt]")
parser.add_argument("output_hdfs", help="Output folder path in Hadoop [e.g. out]")
parser.add_argument("centroids_hdfs", help="Centroids file path in hadoop [e.g. DC/centroids.txt] Folder must exist regardless of random option or seed [File may not exist for random option]")
parser.add_argument("-r", "--random", help="Flag to generate random inital centroids from hadoop file path [Default use centroids already in centroids_hdfs path]",action="store_true")
parser.add_argument("-maxIter", help="Maximum Number of Iterations [Default is 3]",default = 3, type=int)
parser.add_argument("-nClusters", help="Number of Clusters [Defalut is 3], number must match number centroids in file in centroids_hdfs ",default = 3, type=int)

args = parser.parse_args()

# option to random seed or provide one

delim = '/'
centroidsPathList = args.centroids_hdfs.split(delim)
centroidsFileName = centroidsPathList[-1]
centroidsFilePath = delim.join(centroidsPathList[0:-1])

# call function to generate initial centroids if no flag
if args.random:
    os.system("python randomCentroids.py " + args.input_hdfs + " " + args.centroids_hdfs + " -nClusters " + str(args.nClusters))


# iteratre until tolerance error
for i in range(args.maxIter):
    print(".....Iteration: {0} of {1}".format(i+1,args.maxIter))

    
    # remove output folder in hadoop
    os.system("hadoop fs -rm -r " + args.output_hdfs)
    # run map reduce job
    os.system("hadoop jar lin_rehman_exercise.jar " + args.input_hdfs + " " + args.output_hdfs + " " + args.centroids_hdfs)
    # combine outputs to local
    os.system("hadoop fs -getmerge " + args.output_hdfs + " " + centroidsFileName )
    # remove crc file http://stackoverflow.com/questions/15434709/checksum-exception-when-reading-from-or-copying-to-hdfs-in-apache-hadoop
    os.system("rm " + "." + centroidsFileName + ".crc")
    # remove old centroids from hadoop
    os.system("hadoop fs -rm " + args.centroids_hdfs)
    # copy updated centroids to hadoop
    os.system("hadoop fs -put " + centroidsFileName + " " + centroidsFilePath)
    

print ("......Output File ({0}) located in current directory".format(centroidsFileName))



    

