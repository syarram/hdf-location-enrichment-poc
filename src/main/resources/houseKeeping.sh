spark-submit --verbose --master yarn-client \
--driver-memory 1g \
--executor-memory 1g \
--conf spark.sql.shuffle.partitions=10 \
--jars .m2/repository/com/hortonworks/shc-core/1.1.1-2.1-s_2.11/shc-core-1.1.1-2.1-s_2.11.jar,.m2/repository/org/apache/htrace/htrace-core/3.1.0-incubating/htrace-core-3.1.0-incubating.jar,Projects/spark-jars/hbase-client-1.1.2.jar,Projects/spark-jars/hbase-server-1.1.2.jar,Projects/spark-jars/hbase-protocol-1.1.2.jar,Projects/spark-jars/hbase-common-1.1.2.jar,.m2/repository/org/apache/hbase/connectors/spark/hbase-spark/1.0.0/hbase-spark-1.0.0.jar,.m2/repository/org/apache/hbase/hbase-shaded-mapreduce/2.2.2/hbase-shaded-mapreduce-2.2.2.jar,Projects/spark-jars/hadoop-lzo.jar \
--files /hbase-1.1.2/conf/hbase-site.xml,/hadoop-2.7.6/etc/hadoop/core-site.xml \
--driver-library-path /hadoop-2.7.6/lib/native/Mac_OS_X-x86_64-64 \
--class com.tef.etl.main.HouseKeeping /Projects/hdf-location-enrichment-poc/target/etl_batch-0.1.jar \
stage_mme \
MME \
controlTableName \
DEBUG \
10 \
8 \
true