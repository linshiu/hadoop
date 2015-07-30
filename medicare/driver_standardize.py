# -*- coding: utf-8 -*-
"""
Driver to standardize

@author: steven
"""

import argparse
import os
import sys
input_hdfs='assignment3/medicare/Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt'
output_hdfs='standardized'
# remove output folder in hadoop
os.system("python standardize.py " + input_hdfs + " " + output_hdfs)