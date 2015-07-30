# Temperature

* Mapreduce job in java that will calculate the maximum temperate for each year. The output is pairs (year,temperature).

* The data is from the NOAA web site.Example record: 
	`0029029070999991901010106004+64333+023450FM- 12+000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999`

* Year = positions at 15-19, In this example: 1901
* Temperature = positions at 87-92, In this example: -0078

* If Temperature = 9999, it should be interpreted as missing value. The temperature quality is in position 92-93. If it is in the range {0,1,4,5,9}, then the temperature reading is accurate and satisfactory.