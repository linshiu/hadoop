# -*- coding: utf-8 -*-
"""
Python Script to standardize data using pig
Calls pig script from which python script is running
Creates standardized copy in local directory
Puts a copy in hadoop in same folder as input with standardized_ as prefix of input file

@author: steven
"""

import argparse
import os
import sys
parser = argparse.ArgumentParser(description='*** Standardize using Pig ***')
parser.add_argument("input_hdfs", help="Input FILE path in Hadoop with dataset to standardize")
parser.add_argument("output_hdfs", help="Output FOLDER path in Hadoop where folder output of pig will be saved")
args = parser.parse_args()

delim = '/'
inputPathList = args.input_hdfs.split(delim)
inputFileName = inputPathList[-1]
inputFilePath = delim.join(inputPathList[0:-1])
standardFileName = "standardized_" + inputFileName 

# remove output folder in hadoop
os.system("hadoop fs -rm -r " + args.output_hdfs)

# call pig script
os.system("pig -param input_hdfs=" +  args.input_hdfs + " -param output_hdfs=" + args.output_hdfs + " standardize.pig")

# combine outputs to local
os.system("hadoop fs -getmerge " + args.output_hdfs + " " + standardFileName)

# remove crc file http://stackoverflow.com/questions/15434709/checksum-exception-when-reading-from-or-copying-to-hdfs-in-apache-hadoop
os.system("rm " + "." + standardFileName + ".crc")

# remove standardized file in hadoop in case already exists
os.system("hadoop fs -rm " + inputFilePath + "/" + standardFileName)

# copy to hadoop
os.system("hadoop fs -put " + standardFileName + " " + inputFilePath)

# remove output folder
os.system("hadoop fs -rm -r " + args.output_hdfs)

print("....standardized file named " + standardFileName + " in " + inputFilePath + " (hadoop) and local directory")
