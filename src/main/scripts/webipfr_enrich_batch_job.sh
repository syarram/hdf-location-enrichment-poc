#!/bin/bash

yday=`date -d yesterday '+%Y%m%d'`

location_table=stage_mme
transaction_table=stage_weblogs
radius_table=stage_radius
hdfs_magnet=/data/magnet/dt=20200427
hdfs_devicedb=/user/yarrams1/data/DeviceDB/dt=20210107
hdfs_enrichment=/data/a2_prod/web_a2_prod/
csp_table=csp_apn_lkp
control_table=webipfr_enrich_control
log_date=`date +%d%m%Y_%H%M%S`
log_dir=/home/yarrams1/ipfr-load/logs


#submit_saprk_job
spark-submit \
--verbose \
--master yarn \
--driver-memory 15g \
--executor-memory 10g \
--conf spark.sql.shuffle.partitions=200 \
--jars /home/yarrams1/ipfr-load/jars/shc-core-1.1.1-2.1-s_2.11.jar,/home/yarrams1/ipfr-load/jars/hbase-spark-1.0.0.jar,/usr/hdp/current/hbase-client/lib/* \
--files /usr/hdp/current/hbase-client/conf/hbase-site.xml,/etc/hadoop/conf/core-site.xml \
--class com.tef.etl.main.WebIpfrBatchEnrich \
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