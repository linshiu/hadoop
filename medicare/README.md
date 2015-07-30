# Medicare

**Team: Steven Lin, Ahsan Rehman**

![Cluster](https://github.com/linshiu/hadoop/blob/master/medicare/1_Visuals/clusters.png)

- Map reduce job that will execute a single iteration of k-means
- External script that will call this map reduce job many times. The script takes the output of
the previous iteration, use it as input to map reduce.
- Distributed cache concept used. 

[More Info Dataset](https://github.com/linshiu/hadoop/blob/master/medicare/0_Info/Medicare-Physician-and-Other-Supplier-PUF-Methodology.pdf)

[Report](/medicare/2_Final%20Output/lin_rehman_report.pdf)

## 1. Standardization:

 - driver_standardize.py
 - standardize.py
 - standardize.pig
 
The driver calls the other two files and outputs a standardized dataset with only the relevant columns
to cluster, uploads the file to hadoop. 

## 2. K-means

 - driverLoop_kMeans.py
 - kMeans.py
 - randomCentroids.py
 - lin_rehman_exercise.java
 - lin_rehman_exercise.jar

The driver loops through cluster size and number of initial random centroids by calling kMeans.py
kMeans.py calls randomCentroids.py, and then lin_rehman_exercise.jar to run mapreduce job 

## 3. Standardization:

 - driver_standardize2.py
 - standardize2.py
 - standardize2.pig

The driver calls the other two files and outputs a standardized dataset with only the relevant columns
to cluster and additional demographic information, uploads the file to hadoop. 

## 4. Profiling

 - driverKmeans.py
 - profile.py
 - lin_rehman_exercise2.java
 - lin_rehman_exercise2.jar

Driver runs a map reduce job to get the centroids
profile.py runs map reduce job where assigns centroids to file. 

profile.py calls the other files to run map reduce job for the given centroids.
It otuputs the ddata set from 3. with the cluster assignment
