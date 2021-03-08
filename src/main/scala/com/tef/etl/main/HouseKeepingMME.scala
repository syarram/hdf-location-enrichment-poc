package com.tef.etl.main

import com.tef.etl.SparkFuncs.{SparkSessionTrait, SparkUtils}
import com.tef.etl.SparkFuncs.SparkUtils.{part_dt, part_hour}
import com.tef.etl.catalogs.HBaseCatalogs
import com.tef.etl.main.WebIpfrBatchEnrich.spark
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Delete, HTable, Put}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.slf4j.{Logger, LoggerFactory}




object HouseKeepingMME {

  def main(args: Array[String]): Unit = {

    import spark.implicits._

    val format = "org.apache.spark.sql.execution.datasources.hbase"
    val locationTable = "\"" + args(0) + "\""
    val locationTableConnector = args(0)
    val webIpfrEnrichControl = "\"" + args(1) + "\""
    val webIpfrEnrichControlConnector = args(1)
    val errorLogging = args(2)
    val deleteBatchSize = args(3).toInt
    val deleteOlderThanHours = args(4).toInt
    val runDeleFlag = args(5).toBoolean
    val showCountFlag = args(6).toBoolean


    val logger = LoggerFactory.getLogger(WebIpfrBatchEnrich.getClass)
    val locCatalog = HBaseCatalogs.mmecatalog(locationTable)
    val controlCatalog = HBaseCatalogs.webipfr_enrich_control(webIpfrEnrichControl)


    val locationDF = (SparkUtils.reader(format, locCatalog)(spark))//.filter(col("userid_web").isNotNull)

    val webIpfrEnrichControlDF = SparkUtils.reader(format, controlCatalog)(spark)
    val streamProccessedTimeVal = SparkUtils.colValFromDF(webIpfrEnrichControlDF, "weblogs_batch_processed_ts")(spark)
    val deleteToTimeStamp = streamProccessedTimeVal.toLong - (deleteOlderThanHours*60*60*1000)
    val deleteFromTimeStamp = deleteToTimeStamp - (5*24*60*60*1000)



    val timeRangeMMEDF = SparkUtils.mmeReader(format,locCatalog, deleteFromTimeStamp.toString, deleteToTimeStamp.toString)(spark)
    val timeRangeMMEDFLkeys = timeRangeMMEDF.select(col("userid_mme_seq"))

    if(showCountFlag)
     println("******************************** Count of Records"+ timeRangeMMEDFLkeys.count)

    if(runDeleFlag) {
      val conf = HBaseConfiguration.create()
      val hbaseContext = new HBaseContext(spark.sparkContext, conf)
      val webBothDFsRDD = timeRangeMMEDFLkeys.select(col("userid_mme_seq")).map(row => row.getAs[String]("userid_mme_seq").getBytes).rdd
      hbaseContext.bulkDelete[Array[Byte]](webBothDFsRDD, TableName.valueOf(locationTableConnector), deleteRecord => new Delete(deleteRecord), deleteBatchSize)
    }
    //   println("**************** count of records in location:  "+locationDF.count+" Batch Processed Val: "+streamProccessedTimeVal)


 //updating control table with timestamp
 val config = HBaseConfiguration.create
    // instantiate HTable class
    val controlTable = new HTable(config, webIpfrEnrichControlConnector)

    // instantiate Put class
    var put = new Put(Bytes.toBytes("1000000"))
    // add values using add() method
    put.add(Bytes.toBytes("cfEnrich"), Bytes.toBytes("mme_deleted_ts"), Bytes.toBytes(streamProccessedTimeVal))
    controlTable.put(put)

  }

}
