package com.tef.etl.main

import com.tef.etl.SparkFuncs.SparkSessionTrait
import com.tef.etl.catalogs.HBaseCatalogs
import com.tef.etl.model.Definitions
import com.tef.etl.weblogs.{TransactionDFOperations, Utils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/**
 * This spark job Filter records that are not processed by Structured Stream based on WeblogsStreamProcessedTS.
 * and For records where Lkey not populated, join with MME table and populates LKey.
 * This job also extract columns string and populate individual columns for all records.
 * Then it Insert Fully enriched records to HDFS.
 * After successful insert to HDFS, delete those records in HBASE Weblogs table
 * After successful delete, delete data from WeblogsTemp table.
 */
object WebIpfrBatchEnrich extends SparkSessionTrait {

  def main(args: Array[String]): Unit = {
    val format = "org.apache.spark.sql.execution.datasources.hbase"
    val locationTable = args(0)
    val transactionTable = args(1)
    val magnetPath = args(2).split("=")(0)
    val magnetPathDate = args(2).split("=")(1)
    val deviceDBPath = args(3).split("=")(0)
    val deviceDBPathDate = args(3).split("=")(1)
    val cspTable = args(4)
    val radiusTable = args(5)
    val logType = args(6)
    val hdfsPartitions = args(7).toInt
    val enrichPath = args(8)
    val errorPath = args(9)
    val controlTable = args(10)
    val dateLimit = args(11)
    val deleteFlag = args(12)
    val filterByBatchProcessTime = args(13)


    val logger = LoggerFactory.getLogger(WebIpfrBatchEnrich.getClass)

    logger.info("**********************Argument/Variables*************************************")
    logger.info(s"locationTable=>$locationTable")
    logger.info(s"transactionTable=>$transactionTable")
    logger.info(s"magnetPath=>$magnetPath")
    logger.info(s"magnetPathDate=>$magnetPathDate")
    logger.info(s"deviceDBPath=>$deviceDBPath")
    logger.info(s"deviceDBPath=>$deviceDBPathDate")
    logger.info(s"cspTable=>$cspTable")
    logger.info(s"radiusTable=>$radiusTable")
    logger.info(s"logType=>$logType")
    logger.info(s"hdfsPartitions=>$hdfsPartitions")
    logger.info(s"enrichPath=>$enrichPath")
    logger.info(s"errorPath=>$errorPath")
    logger.info(s"controlTable=>$controlTable")
    logger.info(s"dateLimit=>$dateLimit")
    logger.info(s"deleteFlag=>$deleteFlag")
    logger.info(s"filterByBatchProcessTime=>$filterByBatchProcessTime")
    logger.info("*********************Argument/Variables*************************************")

    import spark.implicits._
    spark.sparkContext.setLogLevel(logType)

    val houseKeepingCtlCtlg = HBaseCatalogs.houseKeepingCatalog("\""+controlTable+"\"","Web")
    val hkCntrlDF = Utils.reader(format, houseKeepingCtlCtlg)(spark).cache()
    val batchStatus = Utils.colValFromDF(hkCntrlDF, "batch_job_status")(spark)
    if (batchStatus.equals("InProgress")) {
      logger.error("************************ Batch job in progress, Job will be terminated")
      spark.stop
      spark.close
    } else {
      val updatedCntlDF = hkCntrlDF.withColumn("batch_job_status", lit("InProgress"))
      Utils.updateHbaseColumn(houseKeepingCtlCtlg,updatedCntlDF)
    }

    val controlCatalog = HBaseCatalogs.controlCatalog("\"" + controlTable + "\"")
    val controlDF = Utils.reader(format, controlCatalog)(spark).cache()
    val streamProccessedTimeVal = Utils.colValFromDF(controlDF, "weblogs_stream_processed_ts")(spark)
    val batchProccessedTimeVal = Utils.colValFromDF(controlDF, "weblogs_batch_processed_ts")(spark)


    val webCatalog = HBaseCatalogs.stagewebcatalog("\"" + transactionTable + "\"")
    val sourceDF = (Utils.reader(format, webCatalog)(spark))
    val sourceDFFiltered = if(filterByBatchProcessTime.equalsIgnoreCase("true"))
     sourceDF.filter((col("time_web") <= streamProccessedTimeVal) && (col("time_web") > batchProccessedTimeVal)).repartition(hdfsPartitions).cache()
     else
      sourceDF.filter(col("time_web") <= streamProccessedTimeVal).repartition(hdfsPartitions).cache()

    val webCount = sourceDFFiltered.count()

    if (webCount == 0) {
      logger.error("************************ WebCount is Zero, Job will be terminated")
      //update timetamp & status
      val updatedCntlDF = hkCntrlDF.withColumn("weblogs_batch_processed_ts", lit(streamProccessedTimeVal))
        .withColumn("batch_job_status", lit("Completed"))
      Utils.updateHbaseColumn(houseKeepingCtlCtlg, updatedCntlDF)
      spark.stop
      spark.close
    }
    else logger.info("************************WebCount: " + webCount)

    val mmeCatalog = HBaseCatalogs.mmecatalog("\"" + locationTable + "\"")
    val locationDF = Utils.reader(format, mmeCatalog)(spark)

    val hadoopConf: Configuration = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    val maxMagOdate = Utils.getAmendedDate(magnetPathDate,-dateLimit.toInt)
    val magnetPartition = Utils.getLastPartition(fs, magnetPath, magnetPathDate, maxMagOdate)
    if (magnetPartition.equals("NotFound")){
      logger.error("************************ Magnet partition is not found, Job will be terminated")
      spark.stop
      spark.close
    }
    val magnetSrcDF = Utils.readLZO(spark, magnetPartition, "\t", Definitions.magnetSchema)
        .dropDuplicates("lkey")
    val magnetDF = magnetSrcDF.select(Definitions.magnetSelectList.map(col): _*).cache()

    val maxDDBOdate = Utils.getAmendedDate(deviceDBPathDate,-dateLimit.toInt)
    val DDBPartition = Utils.getLastPartition(fs, deviceDBPath, deviceDBPathDate, maxDDBOdate)
    if (DDBPartition.equals("NotFound")){
      logger.error("************************ Device DB partition is not found, Job will be terminated")
      spark.stop
      spark.close
    }
    val deviceSrcDBDF = Utils.readLZO(spark, DDBPartition, "\t", Definitions.deviceDBSchema).cache()
    val deviceDBDF = deviceSrcDBDF.select(Definitions.deviceDBSelectList.map(col): _*).cache()

    val cspCatalog = HBaseCatalogs.cspCatalog("\"" + cspTable + "\"")
    val cspDF = (Utils.reader(format, cspCatalog)(spark)).select("ip", "csp", "apnid").cache()

    val RadiusCatalog = HBaseCatalogs.stageRadiusCatalog("\"" + radiusTable + "\"")
    val radiusDF = (Utils.reader(format, RadiusCatalog)(spark))
      .withColumn("sesID",concat(split(col("rkey"),":")(0),lit(":")))
      .withColumn("seq",
      when(col("cc").equalTo("PCRF_CCAT"),1)
        .when(col("cc").equalTo("PCRF_RART"),2)
        .when(col("cc").equalTo("PCRF_CCAI"),3)
        .otherwise(4))
    val windowSpec  = Window.partitionBy("sesID").orderBy("seq")
    val radiusSRCDF = radiusDF.withColumn("rank",rank().over(windowSpec)).filter(col("rank")===1).cache()

    //Filter all rows with empty or null nonlkey_cols value
    val srcFilteredDF = sourceDFFiltered.filter(col("nonlkey_cols").isNotNull)
    logger.info(s"********number of records with nonlkey_cols ${srcFilteredDF.count()}")

    val sourceDFWithLkey = srcFilteredDF.filter(
      col("lkey_web").notEqual("Unknown") &&
        col("lkey_web").notEqual("NoMatch") &&
        col("lkey_web").notEqual("NoLocation") )
      .withColumnRenamed("lkey_web", "lkey")

    val transWithLkeyOtherTables = TransactionDFOperations.joinForLookUps(sourceDFWithLkey, magnetDF, deviceDBDF, cspDF, radiusSRCDF)
    val transWithLkeyOtherTablesExpanded = TransactionDFOperations.sourceColumnSplit(spark, transWithLkeyOtherTables,
      "WEB")
    val transWithLkeyOtherTablesExpandedFinal = TransactionDFOperations.getFinalDF(transWithLkeyOtherTablesExpanded)
    Utils.writeErichedData(transWithLkeyOtherTablesExpandedFinal,enrichPath,errorPath)

    val sourceDFWithoutLkey = srcFilteredDF.filter(
      col("lkey_web") === "Unknown" ||
        col("lkey_web") === "NoMatch"
    )
    val sourceMMEJoinedDF = TransactionDFOperations.joinWithMME(sourceDFWithoutLkey, locationDF, hdfsPartitions)
    val transWithMMELkeyOtherTables = TransactionDFOperations.joinForLookUps(sourceMMEJoinedDF, magnetDF, deviceDBDF, cspDF, radiusSRCDF)
    val transWithMMELkeyOtherTablesExpanded = TransactionDFOperations.sourceColumnSplit(spark, transWithMMELkeyOtherTables, "WEB")
    val transWithMMELkeyOtherTablesExpandedFinal = TransactionDFOperations.getFinalDF(transWithMMELkeyOtherTablesExpanded)
    Utils.writeErichedData(transWithMMELkeyOtherTablesExpandedFinal,enrichPath,errorPath)

    val srcDFWithNoLoc = srcFilteredDF.filter(col("lkey_web") === "NoLocation").
      withColumn("lkey", lit("1090-79999"))
    val noLOCKeyOtherTables = TransactionDFOperations.joinNoLOCLookUps(srcDFWithNoLoc, cspDF, radiusSRCDF)
    val transWithNoLOCKeyExpanded = TransactionDFOperations.sourceColumnSplit(spark,
      noLOCKeyOtherTables, "WEB")
    val transWithNoLOCKeyExpandedFinal = TransactionDFOperations.getFinalDFForNoLOC(transWithNoLOCKeyExpanded)
    Utils.writeErichedData(transWithNoLOCKeyExpandedFinal,enrichPath,errorPath)

    if(deleteFlag.equalsIgnoreCase("true")){
      val HbaseConf = HBaseConfiguration.create()
      val hbaseContext = new HBaseContext(spark.sparkContext, HbaseConf)
      val keysDF = sourceDFFiltered.select(col("userid_web_seq"))
      val webBothDFsRDD = keysDF.map(row => row.getAs[String]("userid_web_seq").getBytes).rdd
      hbaseContext.bulkDelete[Array[Byte]](webBothDFsRDD, TableName.valueOf(transactionTable), deleteRecord => new Delete
      (deleteRecord), 4)
    }

    //Update weblogs_batch_processed_ts with weblogs_stream_processed_ts
    val updatedCntlDF = hkCntrlDF.withColumn("weblogs_batch_processed_ts", lit(streamProccessedTimeVal))
      .withColumn("batch_job_status", lit("Completed"))
    Utils.updateHbaseColumn(houseKeepingCtlCtlg, updatedCntlDF)

    spark.close()
  }
}
