#!/bin/bash


if [ $# -ne 1 ]; then
    echo "************ Need Environment Name test OR prod *****Job failed ${?}"
    exit 1
fi

env=${1}

echo "*************************Params passed: ${env} "

if [ ${env} == "test" ]; then
kafka_bootstrap=uds-ct1-kq7.ref.dab.02.net:6667,uds-ct1-kq8.ref.dab.02.net:6667,uds-ct1-kq9.ref.dab.02.net:6667,uds-ct1-kq10.ref.dab.02.net:6667
hbase_quorum=uds-ct1-mn1.ref.dab.02.net,uds-ct1-mn2.ref.dab.02.net,uds-ct1-mn3.ref.dab.02.net
else
kafka_bootstrap=uds-ct1-kq7.ref.dab.02.net:6667,uds-ct1-kq8.ref.dab.02.net:6667,uds-ct1-kq9.ref.dab.02.net:6667,uds-ct1-kq10.ref.dab.02.net:6667
hbase_quorum=uds-ct1-mn1.ref.dab.02.net,uds-ct1-mn2.ref.dab.02.net,uds-ct1-mn3.ref.dab.02.net
fi

control_table=webipfr_enrich_control
target_table=stage_weblogs
log_type=ERROR
trans_topic=stageweblogs
location_topic=stagemme
offset_read=latest
max_executors=100
min_executors=30
log_date=`date +%d%m%Y_%H%M%S`
log_dir=/home/yarrams1/ipfr-load/logs



spark-submit --class com.telefonica.netpulse.enrichment.EnrichTransactionStream --num-executors 4 --executor-memory 10G --driver-memory 20g \
--conf spark.sql.shuffle.partitions=100 --conf spark.dynamicAllocation.minExecutors=${min_executors} --conf spark.dynamicAllocation.maxExecutors=${max_executors} \
--conf spark.executor.heartbeatInterval=20s --master yarn \
--jars /usr/hdp/2.6.5.0-292/hbase/lib/*.jar,/usr/hdp/2.6.5.0-292/kafka/libs/kafka-clients-1.0.0.2.6.5.0-292.jar,jars/spark-sql-kafka-0-10_2.11-2.3.0.2.6.5.0-292.jar \
ipfr-enrichment-1.0-SNAPSHOT.jar --ipfr-topic ${trans_topic} --ipfr-starting-offset ${offset_read} \
--mme-topic ${location_topic} --mme-starting-offset ${offset_read} \
--kafka-bootstrap-servers ${kafka_bootstrap} \
--error-logging ERROR --aggregation-window-time 15 --join-window-time 15 --mme-water-mark 15 --ipfr-water-mark 15 \
--target-table stage_weblogs \
--loc-kafka-offsets-trigger 40000000 \
--trans-kafka-offsets-trigger 20000000 \
--hbase-zookeeper-quorum ${hbase_quorum}  \
--hbase-zookeeper-property-clientPort 2181  --zookeeper-znode-parent /hbase-unsecure --target-table-cf cfLKey \
--target-table-lkey-column lkey --target-table-lkey-nomatch NoMatch --start-partition 0 --end-partition 30 \
--control-table ${control_table} --control-table-cf cfEnrich \
--control-table-column weblogs_stream_processed_ts \
--control-table-rowkey 1000000 >> $log_dir/spark_streaming_weblogs_$log_date.log 2>&1 &