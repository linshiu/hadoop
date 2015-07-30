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
	$4, $5, $6, $9, $11, $12,$13,$14,$15,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26;

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
	MAX(subset.$10),
	MAX(subset.$11),
	MAX(subset.$12),
	MAX(subset.$13),
	MAX(subset.$14),
	MAX(subset.$15), 
	MAX(subset.$16), 
	MAX(subset.$17),
	MAX(subset.$18);

min = FOREACH grouped GENERATE
	MIN(subset.$10),
	MIN(subset.$11),
	MIN(subset.$12),
	MIN(subset.$13),
	MIN(subset.$14),
	MIN(subset.$15), 
	MIN(subset.$16), 
	MIN(subset.$17),
	MIN(subset.$18);

--DUMP max;'

standardized = FOREACH subset GENERATE
			$0, $1, $2, $3, $4, $5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
			($10 - min.$0)/(max.$0 - min.$0),
			($11 - min.$1)/(max.$1 - min.$1),
			($12 - min.$2)/(max.$2 - min.$2),
			($13 - min.$3)/(max.$3 - min.$3),
			($14 - min.$4)/(max.$4 - min.$4),
			($15 - min.$5)/(max.$5 - min.$5),
			($16 - min.$6)/(max.$6 - min.$6),
			($17 - min.$7)/(max.$7 - min.$7),
			($18 - min.$8)/(max.$8 - min.$8);

--output_hdfs= standardized
STORE standardized INTO '$output_hdfs' USING PigStorage('\t');