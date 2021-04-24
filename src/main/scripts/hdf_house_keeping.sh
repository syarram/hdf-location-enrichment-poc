#!/bin/bash



location_table=stage_mme_20210415_bkp
catalog=${1}
if [
control_table=webipfr_enrich_control
log_type=INFO
delete_batch_size=10000000
older_hours=${2}
delete_flag=true
log_date=`date +%d%m%Y%H%M%S`
log_dir=/home/yarrams1/hdflogs



spark-submit --verbose --master yarn-client --driver-memory 15g --executor-memory 10g  --num-executors 4
--executor-cores 5 --jars /home/yarrams1/ipfr-load/jars/shc-core-1.1.1-2.1-s_2.11.jar,/usr/hdp/current/hbase-client/lib/*
--files /usr/hdp/current/hbase-client/conf/hbase-site.xml,/etc/hadoop/conf/core-site.xml
--class com.tef.etl.main.HouseKeeping /home/yarrams1/ipfr-load/etl_batch-0.1.jar stage_mme_20210415_bkp MME webipfr_enrich_control DEBUG 1000000 8 true



#submit_saprk_job
spark-submit \
--verbose \
--master yarn \
--driver-memory 15g \
--executor-memory 10g \
--conf spark.sql.shuffle.partitions=200 \
--jars /home/yarrams1/ipfr-load/jars/shc-core-1.1.1-2.1-s_2.11.jar,/usr/hdp/current/hbase-client/lib/* \
--files /usr/hdp/current/hbase-client/conf/hbase-site.xml,/etc/hadoop/conf/core-site.xml \
--class class com.tef.etl.main.HouseKeeping \
/home/yarrams1/ipfr-load/etl_batch-0.1.jar \
${location_table} \
${transaction_table} \
${hdfs_magnet} \
${hdfs_devicedb} \
${csp_table} \
${radius_table} \
INFO \
100 \
${hdfs_enrichment} \
${control_table} >> $log_dir/spark_job_trigger_$log_date.log 2>&1 &