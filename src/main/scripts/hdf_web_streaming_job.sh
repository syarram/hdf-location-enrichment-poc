#!/bin/bash


if [ $# -ne 2 ]; then
    echo "************ Need Environment Name test OR prod *****AND offset value, earliest or latest ****Job failed ${?}"
    exit 1
fi

env=${1}
offset_read=${2}


echo "*************************Params passed: ${env} and ${offset_read}"

if [ ${env} == "test" ]; then
kafka_bootstrap=uds-ct1-kq7.ref.dab.02.net:6667,uds-ct1-kq8.ref.dab.02.net:6667,uds-ct1-kq9.ref.dab.02.net:6667,uds-ct1-kq10.ref.dab.02.net:6667
hbase_quorum=uds-ct1-mn1.ref.dab.02.net,uds-ct1-mn2.ref.dab.02.net,uds-ct1-mn3.ref.dab.02.net
else
kafka_bootstrap=uds-far-kq1.dab.02.net:6667,uds-far-kq2.dab.02.net:6667,uds-far-kq3.dab.02.net:6667,uds-far-kq4.dab.02.net:6667,uds-far-kq5.dab.02.net:6667,uds-far-kq6.dab.02.net:6667,uds-far-kq7.dab.02.net:6667,uds-far-kq8.dab.02.net:6667,uds-far-kq9.dab.02.net:6667,uds-far-kq10.dab.02.net:6667,uds-far-kq11.dab.02.net:6667,uds-far-kq12.dab.02.net:6667
hbase_quorum=uds-far-mn1.dab.02.net,uds-far-mn2.dab.02.net,uds-far-mn3.dab.02.net
fi


hdf_home=/app/hdf_a2
control_table=webipfr_enrich_control
target_table=stage_weblogs_20210513
log_type=ERROR
trans_topic=weblogs20210513
location_topic=mme20210513
max_executors=300
min_executors=30
log_date=`date +%d%m%Y_%H%M%S`
log_dir=$hdf_home/logs



spark-submit --class com.telefonica.netpulse.enrichment.EnrichTransactionStream --num-executors 4 --executor-memory 10G --driver-memory 20g \
--conf spark.sql.shuffle.partitions=100 --conf spark.dynamicAllocation.minExecutors=${min_executors} --conf spark.dynamicAllocation.maxExecutors=${max_executors} \
--conf spark.executor.heartbeatInterval=20s --master yarn \
--jars /usr/hdp/2.6.5.0-292/hbase/lib/*.jar,/usr/hdp/2.6.5.0-292/kafka/libs/kafka-clients-1.0.0.2.6.5.0-292.jar,/app/hdf_a2/jars/spark-sql-kafka-0-10_2.11-2.3.0.2.6.5.0-292.jar \
${hdf_home}/ipfr-enrichment-1.0-SNAPSHOT.jar --ipfr-topic ${trans_topic} --ipfr-starting-offset ${offset_read} \
--mme-topic ${location_topic} --mme-starting-offset ${offset_read} \
--kafka-bootstrap-servers ${kafka_bootstrap} \
--error-logging ERROR --aggregation-window-time 15 --join-window-time 15 --mme-water-mark 15 --ipfr-water-mark 15 \
--target-table ${target_table} \
--loc-kafka-offsets-trigger 40000000 \
--trans-kafka-offsets-trigger 20000000 \
--hbase-zookeeper-quorum ${hbase_quorum}  \
--hbase-zookeeper-property-clientPort 2181  --zookeeper-znode-parent /hbase-unsecure --target-table-cf cfLKey \
--target-table-lkey-column lkey --target-table-lkey-nomatch NoMatch --start-partition 0 --end-partition 30 \
--control-table ${control_table} --control-table-cf cfEnrich \
--control-table-column weblogs_stream_processed_ts \
--control-table-rowkey 1000000 >> $log_dir/spark_streaming_weblogs_$log_date.log 2>&1 &