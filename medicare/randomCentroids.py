"""
Python Script to generate random centroids to initialize k-means

@author: steven
"""

import argparse
import os
import sys
parser = argparse.ArgumentParser(description='*** Random Centroids ***')
parser.add_argument("input_hdfs", help="Input folder [or file] path in Hadoop [e.g. input OR input/data.txt]")
parser.add_argument("centroids_hdfs", help="Centroids file path in hadoop [e.g. DC/centroids.txt] Folder must exist regardless of random option or seed [File may not exist for random option]")
parser.add_argument("-nClusters", help="Number of Clusters [Defalut is 3], number must match number centroids in file in centroids_hdfs ",default = 3, type=int)

args = parser.parse_args()

delim = '/'
centroidsPathList = args.centroids_hdfs.split(delim)
centroidsFileName = centroidsPathList[-1]
centroidsFilePath = delim.join(centroidsPathList[0:-1])

inputPathList = args.input_hdfs.split(delim)
inputFileName = inputPathList[-1]
inputFilePath = delim.join(inputPathList[0:-1])

print "Generating random initial centroids....."

# remove if input file from current local directory if already exists in local
os.system("rm " + inputFileName)
# copy input file from hadoop to local
os.system("hadoop fs -get " + args.input_hdfs)
# remove centroids file in case already exists
os.system("hadoop fs -rm " + args.centroids_hdfs)
# generage random n points and add line number
os.system("shuf -n " + str(args.nClusters) + " " + inputFileName + " | nl > " + centroidsFileName)
# remove leading white space before line number
os.system("sed -i 's/^ *//g' " + centroidsFileName )
#copy file to hadoop
os.system("hadoop fs -put " + centroidsFileName + " " + centroidsFilePath)
# remove input file from local
os.system("rm " + inputFileName)

print ("......Centroids File ({0}) located in current directory and hadoop ({1})".format(centroidsFileName,args.centroids_hdfs))