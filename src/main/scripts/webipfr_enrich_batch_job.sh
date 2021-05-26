#!/bin/bash

yday=`date -d yesterday '+%Y%m%d'`

location_table=stage_mme_20210513
transaction_table=stage_weblogs_20210513
radius_table=stage_radius_20210513
hdfs_magnet=/data/magnet/dt=20210512
hdfs_devicedb=/user/yarrams1/data/DeviceDB/dt=20210512
hdfs_enrichment=/data/a2/web_a2/
csp_table=csp_apn_lkp
control_table=webipfr_enrich_control
log_date=`date +%d%m%Y_%H%M%S`
log_dir=/app/hdf_a2/logs
max_executors=400
min_executors=150


#submit_saprk_job
spark-submit \
--verbose \
--master yarn \
--driver-memory 15g \
--executor-memory 10g \
--conf spark.sql.shuffle.partitions=200 --conf spark.dynamicAllocation.minExecutors=${min_executors} --conf spark.dynamicAllocation.maxExecutors=${max_executors} \
--jars /app/hdf_a2/jars/shc-core-1.1.1-2.1-s_2.11.jar,/app/hdf_a2/jars/hbase-spark-1.0.0.jar,/usr/hdp/current/hbase-client/lib/* \
--files /usr/hdp/current/hbase-client/conf/hbase-site.xml,/etc/hadoop/conf/core-site.xml \
--class com.tef.etl.main.WebIpfrBatchEnrich \
/app/hdf_a2/etl_batch-0.1.jar \
${location_table} \
${transaction_table} \
${hdfs_magnet} \
${hdfs_devicedb} \
${csp_table} \
${radius_table} \
INFO \
100 \
${hdfs_enrichment} \
${control_table} >> $log_dir/spark_web_batch_job_$log_date.log 2>&1 &