package com.tef.etl.main

import com.tef.etl.SparkFuncs.SparkUtils
import com.tef.etl.catalogs.HBaseCatalogs
import org.apache.hadoop.hbase.client.{Delete, HTable, Put}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object HouseKeeping {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("HouseKeeping").getOrCreate()

    val format = "org.apache.spark.sql.execution.datasources.hbase"
    val tableName = args(0)
    val catalogName = args(1)
    val controlTableName = args(2)
    val logType = args(3)
    val deleteBatchSize = args(4).toInt
    val deleteOlderThanHours = args(5).toInt
    val runDeleFlag = args(6).toBoolean

    val logger = LoggerFactory.getLogger(HouseKeeping.getClass)
    import spark.implicits._
    spark.sparkContext.setLogLevel(logType)

    logger.info("**********************Argument/Variables*************************************")
    logger.info(s"tableName=>$tableName")
    logger.info(s"catalogName=>$catalogName")
    logger.info(s"controlTableName=>$controlTableName")
    logger.info(s"logType=>$logType")
    logger.info(s"deleteBatchSize=>$deleteBatchSize")
    logger.info(s"deleteOlderThanHours=>$deleteOlderThanHours")
    logger.info(s"runDeleFlag=>$runDeleFlag")
    logger.info("*********************Argument/Variables*************************************")

    var tableCatalog = ""
    var keyColumnName = ""
    var controlColName = ""
    if(catalogName.equals("MME")) {
      tableCatalog = HBaseCatalogs.mmecatalog("\""+tableName+"\"")
      keyColumnName = "userid_mme_seq"
      controlColName = "mme_deleted_ts"
    }else if(catalogName.equals("Radius")){
      tableCatalog = HBaseCatalogs.stageRadiusCatalog("\""+tableName+"\"")
      keyColumnName = "rkey"
      controlColName = "radius_deleted_ts"
    }else {
      logger.error(s"catalogName=>$catalogName doesn't exists or doesn't match criteria")
      spark.stop()
    }

    //read weblogs processed timestamp and derive to and from timestamp
    val controlCatalog = HBaseCatalogs.webipfr_enrich_control("\""+controlTableName+"\"")
    val webIpfrEnrichControlDF = SparkUtils.reader(format, controlCatalog)(spark)
    val streamProccessedTimeVal = SparkUtils.colValFromDF(webIpfrEnrichControlDF, "weblogs_batch_processed_ts")(spark)
    val deleteToTimeStamp = streamProccessedTimeVal.toLong - (deleteOlderThanHours*60*60*1000)
    val deleteFromTimeStamp = deleteToTimeStamp - (5*24*60*60*1000)

    //Get primary keys for given time range
    val timeRangeDF = SparkUtils.mmeReader(format,tableCatalog, deleteFromTimeStamp.toString, deleteToTimeStamp.toString)(spark)
    val timeRangeKeys = timeRangeDF.select(col(keyColumnName))
    logger.debug("******************************** Count of Records"+ timeRangeKeys.count)

    if(runDeleFlag) {
      val conf = HBaseConfiguration.create()
      val hbaseContext = new HBaseContext(spark.sparkContext, conf)
      val webBothDFsRDD = timeRangeKeys.select(col(keyColumnName)).map(row => row.getAs[String](keyColumnName).getBytes).rdd
      hbaseContext.bulkDelete[Array[Byte]](webBothDFsRDD, TableName.valueOf(tableName), deleteRecord => new Delete(deleteRecord), deleteBatchSize)
    }

    //update control table with latest timestamp
    val config = HBaseConfiguration.create
    // instantiate HTable class
    val controlTable = new HTable(config, controlTableName)
    // instantiate Put class
    val put = new Put(Bytes.toBytes("1000000"))
    // add values using add() method
    put.add(Bytes.toBytes("cfEnrich"), Bytes.toBytes(controlColName), Bytes.toBytes(streamProccessedTimeVal))
    controlTable.put(put)

  }

}
