/************
Script standardizes the dataset by (x-xmin)/(xmax-xmin)
Paramters:
	input_hdfs = assignment3/medicare/Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt
	output_hdfs = assignment3/standardized

**************/

-- to call: pig -param parameter1=<parameter value1> parameter2=<parameter value2> pigscript.pig
-- to call: pig -param input_hdfs=medicare/medicare10Sample.txt -param output_hdfs=standardized standardize.pig

--input_hdfs = medicare/medicare10Sample.txt
medicare = LOAD '$input_hdfs' using PigStorage ('\t') as (
	npi: chararray,
	nppes_provider_last_org_name: chararray,
	nppes_provider_first_name: chararray,
	nppes_provider_mi: chararray,
	nppes_credentials: chararray,
	nppes_provider_gender: chararray,
	nppes_entity_code: chararray,
	nppes_provider_street1: chararray,
	nppes_provider_street2: chararray,
	nppes_provider_city: chararray,
	nppes_provider_zip: chararray,
	nppes_provider_state: chararray,
	nppes_provider_country: chararray,
	provider_type: chararray,
	medicare_participation_indicator: chararray,
	place_of_service: chararray,
	hcpcs_code: chararray,
	hcpcs_description: chararray,
	line_srvc_cnt: double, --do count as double because integer divided by integer gives integer
	bene_unique_cnt: double,
	bene_day_srvc_cnt: double,
	average_Medicare_allowed_amt: double,
	stdev_Medicare_allowed_amt: double,
	average_submitted_chrg_amt: double,
	stdev_submitted_chrg_amt: double,
	average_Medicare_payment_amt: double,
	stdev_Medicare_payment_amt: double );

--DUMP medicare;

medicare = filter medicare by $18 is not NULL;

-- don't need to subset, but is more efficient or not?
-- field 18 correspods to line_srvc_cnt (first field is field 0)
subset = FOREACH medicare GENERATE 
	$18,$19,$20,$21,$22,$23,$24,$25,$26;

--DUMP subset;


/*
grouped = GROUP subset all;
max = FOREACH grouped GENERATE
	MAX(subset.line_srvc_cnt),
	MAX(subset.bene_unique_cnt),
	MAX(subset.bene_day_srvc_cnt),
	MAX(subset.average_Medicare_allowed_amt),
	MAX(subset.stdev_Medicare_allowed_amt),
	MAX(subset.average_submitted_chrg_amt), 
	MAX(subset.stdev_submitted_chrg_amt), 
	MAX(subset.average_Medicare_payment_amt),
	MAX(subset.stdev_Medicare_payment_amt);

*/

-- group by one key
grouped = GROUP subset all;

max = FOREACH grouped GENERATE
	MAX(subset.$0),
	MAX(subset.$1),
	MAX(subset.$2),
	MAX(subset.$3),
	MAX(subset.$4),
	MAX(subset.$5), 
	MAX(subset.$6), 
	MAX(subset.$7),
	MAX(subset.$8);

min = FOREACH grouped GENERATE
	MIN(subset.$0),
	MIN(subset.$1),
	MIN(subset.$2),
	MIN(subset.$3),
	MIN(subset.$4),
	MIN(subset.$5), 
	MIN(subset.$6), 
	MIN(subset.$7),
	MIN(subset.$8);

--DUMP max;'

standardized = FOREACH subset GENERATE
			($0 - min.$0)/(max.$0 - min.$0),
			($1 - min.$1)/(max.$1 - min.$1),
			($2 - min.$2)/(max.$2 - min.$2),
			($3 - min.$3)/(max.$3 - min.$3),
			($4 - min.$4)/(max.$4 - min.$4),
			($5 - min.$5)/(max.$5 - min.$5),
			($6 - min.$6)/(max.$6 - min.$6),
			($7 - min.$7)/(max.$7 - min.$7),
			($8 - min.$8)/(max.$8 - min.$8);

--output_hdfs= standardized
STORE standardized INTO '$output_hdfs' USING PigStorage('\t');