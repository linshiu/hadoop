#IBM

* Data set comes from IBM and it is a machine learning classification problem. It’s in the csv format. Code gets average value of the fourth column per every different combination of columns 30,31,32,32 among all records with the last column equal to ‘false’.

* Result looks like:
	1,0,1,1, average value of column 4
	0,0,1,0, average value of column 4
	Etc: for all possible combinations of fields 30,31,32,33

* The result is equivalent to the sql query: select ave(u[4]), u[30],u[31],u[32],u[33] from u group by u[30],u[31],u[32],u[33] where u[last column] = ‘false’ (u[i] is the i’th column)