tpcds-partitioner
=================

TPC-DS data by-date partitioner

To partition TPC-DS data do 

`hadoop jar target/tpcds-parts-1.0-SNAPSHOT.jar -t store_sales -i /user/hive/external/2/store_sales/ -o /user/hive/warehouse/tpcds_bin_partitioned_orc_2.db/store_sales/`

To load the partitions into hive, do

`hive> msck repair table store_sales;`

Please note that the data partitioner does not bucket or enforce global sorting. So do not use clustered-by/sorted-by in the table schema.
