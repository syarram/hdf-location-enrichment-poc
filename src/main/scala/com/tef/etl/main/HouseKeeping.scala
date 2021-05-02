package com.tef.etl.main

import com.tef.etl.SparkFuncs.SparkUtils
import com.tef.etl.catalogs.HBaseCatalogs
import com.tef.etl.weblogs.Utils
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object HouseKeeping {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("HouseKeeping").getOrCreate()

    val format = "org.apache.spark.sql.execution.datasources.hbase"
    val tableName = args(0)
    val processName = args(1)
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
    logger.info(s"processName=>$processName")
    logger.info(s"controlTableName=>$controlTableName")
    logger.info(s"logType=>$logType")
    logger.info(s"deleteBatchSize=>$deleteBatchSize")
    logger.info(s"deleteOlderThanHours=>$deleteOlderThanHours")
    logger.info(s"runDeleFlag=>$runDeleFlag")
    logger.info("*********************Argument/Variables*************************************")

    var tableCatalog = ""
    var keyColumnName = ""
    var controlColName = ""
    var processColName = ""
    if(processName.equals("MME")) {
      tableCatalog = HBaseCatalogs.mmecatalog("\""+tableName+"\"")
      keyColumnName = "userid_mme_seq"
      controlColName = "mme_deleted_ts"
      processColName = "mme_delete_job_status"
    }else if(processName.equals("Radius")){
      tableCatalog = HBaseCatalogs.stageRadiusCatalog("\""+tableName+"\"")
      keyColumnName = "rkey"
      controlColName = "radius_deleted_ts"
      processColName = "radius_delete_job_status"
    }else {
      logger.error(s"processName=>$processName doesn't exists or doesn't match criteria")
      spark.stop()
      spark.close()
    }

    //read weblogs processed timestamp and derive to and from timestamp
    val controlCatalog = HBaseCatalogs.controlCatalog("\""+controlTableName+"\"")
    val controlDF = SparkUtils.reader(format, controlCatalog)(spark).cache()
    val processStatus = SparkUtils.colValFromDF(controlDF, processColName)(spark)
    if(processStatus != null && processStatus.equals("InProgress")){
      logger.error(s"$processName process is still in progress")
      spark.stop()
      spark.close()
    }else{
      val updatedCntlDF = controlDF.withColumn(processColName,lit("InProgress"))
      Utils.updateHbaseColumn(controlCatalog,updatedCntlDF)
    }

    val batchProccessedTimeVal = SparkUtils.colValFromDF(controlDF, "weblogs_batch_processed_ts")(spark)
    val deleteToTimeStamp = batchProccessedTimeVal.toLong - (deleteOlderThanHours*60*60*1000)
    val deleteFromTimeStamp = deleteToTimeStamp - (5*24*60*60*1000)

    //Get primary keys for given time range
    val timeRangeDF = SparkUtils.mmeReader(format,tableCatalog, deleteFromTimeStamp.toString, deleteToTimeStamp.toString)(spark)
    val timeRangeKeys = timeRangeDF.select(col(keyColumnName))
    val timeRangeCnt = timeRangeKeys.count
    if(timeRangeCnt == 0)
    {
      logger.error("************************ Time range count is Zero, Job will be terminated")
      val updatedCntlDF = controlDF.withColumn(controlColName,lit(batchProccessedTimeVal))
        .withColumn(processColName,lit("Completed"))
      Utils.updateHbaseColumn(controlCatalog,updatedCntlDF)
      spark.stop
      spark.close
    }
    else
      logger.info("************************Web Time range count is: "+timeRangeCnt)

    if(runDeleFlag) {
      val conf = HBaseConfiguration.create()
      val hbaseContext = new HBaseContext(spark.sparkContext, conf)
      val webBothDFsRDD = timeRangeKeys.select(col(keyColumnName)).map(row => row.getAs[String](keyColumnName).getBytes).rdd
      hbaseContext.bulkDelete[Array[Byte]](webBothDFsRDD, TableName.valueOf(tableName), deleteRecord => new Delete(deleteRecord), deleteBatchSize)
    }

    val updatedCntlDF = controlDF.withColumn(controlColName,lit(batchProccessedTimeVal))
      .withColumn(processColName,lit("Completed"))
    Utils.updateHbaseColumn(controlCatalog,updatedCntlDF)

    spark.close()
  }

}
