#!/bin/bash

if [ $# -ne 2 ]; then
    echo "************ Need CatalogName and HourstoDelete *****Job failed ${?}"
    exit 1
fi

catalog=${1}
older_hours=${2}

echo "*************Params passed: ${catalog} and ${older_hours} "

if [ ${catalog} == "MME" ]; then
table_name=stage_mme
else
table_name=stage_radius
fi

control_table=webipfr_enrich_control
log_type=INFO
delete_batch_size=10000000
delete_flag=true
log_date=`date +%d%m%Y_%H%M%S`
log_dir=/app/hdf_a2/logs


#submit_saprk_job
spark-submit \
--verbose \
--master yarn \
--driver-memory 15g \
--executor-memory 10g \
--conf spark.sql.shuffle.partitions=200 \
--jars /app/hdf_a2/jars/shc-core-1.1.1-2.1-s_2.11.jar,/app/hdf_a2/jars/hbase-spark-1.0.0.jar,/usr/hdp/current/hbase-client/lib/* \
--files /usr/hdp/current/hbase-client/conf/hbase-site.xml,/etc/hadoop/conf/core-site.xml \
--class com.tef.etl.main.HouseKeeping /app/hdf_a2/etl_batch-0.1.jar \
${table_name} \
${catalog} \
${control_table} \
${log_type} \
${delete_batch_size} \
${older_hours} \
${delete_flag} >> $log_dir/spark_job_hdf_house_keeping_${catalog}_$log_date.log 2>&1 &
