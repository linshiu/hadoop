#Google

## Exercise 1

* Mapreduce java routine (most efficient way with a single mapreduce job) that will for each year report the average number of volumes including words containing substrings: ‘nu’, ‘die’, ‘kla’

* The data represents the number of occurrences of a particular word (or pairs of words) in a given year in all books available by Google Books. The number of different volumes/books including the word (or pairs of words) is also reported.

* The 1gram file format: word \t year \t number of occurrences \t number of volumes \t potential other irrelevant fields

* The 2 gram file format: word \t word \t year \t number of occurrences \t number of volumes \ potential other irrelevant fields

## Exercise 2

* Mapreduce to compute the standard deviation of all volume values (across all records and both files) using not use more than 2 mapreduce jobs.