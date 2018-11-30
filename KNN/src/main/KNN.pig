DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
point_val = load '$input' USING CSVLoader(',') AS (id:chararray, x:double, y:double);
ed = FOREACH point_val GENERATE *, SQRT((x-$point1)*(x-$point1) + (y-$point2)*(y-$point2)) as dist:double;
ascending_order = ORDER ed BY dist ASC;
knn_results = LIMIT ascending_order $k;
STORE knn_results INTO 'OutputPig' USING PigStorage();
DUMP knn_results;
