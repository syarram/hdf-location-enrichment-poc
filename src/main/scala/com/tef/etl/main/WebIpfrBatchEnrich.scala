package com.tef.etl.main

import com.tef.etl.SparkFuncs.{SparkSessionTrait, SparkUtils}
import com.tef.etl.catalogs.HBaseCatalogs
import com.tef.etl.model.Definitions
import com.tef.etl.weblogs.{TransactionDFOperations, Utils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object WebIpfrBatchEnrich extends SparkSessionTrait {

  def main(args: Array[String]): Unit = {
    val format = "org.apache.spark.sql.execution.datasources.hbase"
    val locationTable = args(0)
    val transactionTable = args(1)
    val magnetPath = args(2).split("=")(0)
    val magnetPathDate = args(2).split("=")(1)
    val deviceDBPath = args(3)
    val cspTable = args(4)
    val radiusTable = args(5)
    val logType = args(6)
    val hdfsPartitions = args(7).toInt
    val enrichPath = args(8)
    val controlTable = args(9)


    val logger = LoggerFactory.getLogger(WebIpfrBatchEnrich.getClass)

    logger.info("**********************Argument/Variables*************************************")
    logger.info(s"locationTable=>$locationTable")
    logger.info(s"transactionTable=>$transactionTable")
    logger.info(s"magnetPath=>$magnetPath")
    logger.info(s"magnetPathDate=>$magnetPathDate")
    logger.info(s"deviceDBPath=>$deviceDBPath")
    logger.info(s"cspTable=>$cspTable")
    logger.info(s"radiusTable=>$radiusTable")
    logger.info(s"logType=>$logType")
    logger.info(s"hdfsPartitions=>$hdfsPartitions")
    logger.info(s"enrichPath=>$enrichPath")
    logger.info(s"controlTable=>$controlTable")

    logger.info("*********************Argument/Variables*************************************")

    import spark.implicits._
    spark.sparkContext.setLogLevel(logType)

    val controlCatalog = HBaseCatalogs.controlCatalog("\"" + controlTable + "\"")
    val controlDF = SparkUtils.reader(format, controlCatalog)(spark).cache()
    val batchStatus = SparkUtils.colValFromDF(controlDF, "batch_job_status")(spark)
    if (batchStatus.equals("InProgress")) {
      logger.error("************************ Batch job in progress, Job will be terminated")
      spark.stop
      spark.close
    } else {
      val updatedCntlDF = controlDF.withColumn("batch_job_status", lit("InProgress"))
      Utils.updateHbaseColumn(controlCatalog,updatedCntlDF)
    }
    val streamProccessedTimeVal = SparkUtils.colValFromDF(controlDF, "weblogs_stream_processed_ts")(spark)

    val webCatalog = HBaseCatalogs.stagewebcatalog("\"" + transactionTable + "\"")
    val sourceDF = (SparkUtils.reader(format, webCatalog)(spark))
    val sourceDFFiltered = sourceDF.filter(col("time_web") <= streamProccessedTimeVal).cache()

    val webCount = sourceDFFiltered.count()

    if (webCount == 0) {
      logger.error("************************ WebCount is Zero, Job will be terminated")
      //update timetamp & status
      val updatedCntlDF = controlDF.withColumn("weblogs_batch_processed_ts", lit(streamProccessedTimeVal))
        .withColumn("batch_job_status", lit("Completed"))
      Utils.updateHbaseColumn(controlCatalog, updatedCntlDF)

      spark.stop
      spark.close
    }
    else logger.info("************************WebCount: " + webCount)

    val mmeCatalog = HBaseCatalogs.mmecatalog("\"" + locationTable + "\"")
    val locationDF = SparkUtils.reader(format, mmeCatalog)(spark)

    val hadoopConf: Configuration = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    val magnetPartition = Utils.getLastPartition(fs, magnetPath, magnetPathDate)
    val magnetDF = Utils.readLZO(spark, magnetPartition, "\t", Definitions.magnetSchema)
      .dropDuplicates("lkey").cache()
    val deviceDBDF = Utils.readLZO(spark, deviceDBPath, "\t", Definitions.deviceDBSchema).cache()

    val cspCatalog = HBaseCatalogs.cspCatalog("\"" + cspTable + "\"")
    val cspDF = (SparkUtils.reader(format, cspCatalog)(spark)).select("ip", "csp", "apnid").cache()


    val RadiusCatalog = HBaseCatalogs.stageRadiusCatalog("\"" + radiusTable + "\"")
    val radiusSRCDF = (SparkUtils.reader(format, RadiusCatalog)(spark)).cache()

    val sourceDFWithLkey = sourceDFFiltered.filter(
      col("lkey_web").notEqual("Unknown") &&
        col("lkey_web").notEqual("NoMatch"))
      .withColumnRenamed("lkey_web", "lkey")
    val transWithLkeyOtherTables = TransactionDFOperations.joinForLookUps(sourceDFWithLkey, magnetDF, deviceDBDF, cspDF, radiusSRCDF)
    val transWithLkeyOtherTablesExpanded = TransactionDFOperations.sourceColumnSplit(spark, transWithLkeyOtherTables,
      "WEB")
    val transWithLkeyOtherTablesExpandedFinal = TransactionDFOperations.getFinalDF(transWithLkeyOtherTablesExpanded)
    transWithLkeyOtherTablesExpandedFinal.write.partitionBy("dt", "hour", "loc", "csp")
      .option("codec", "com.hadoop.compression.lzo.LzopCodec")
      .option("delimiter", "\t")
      .mode(SaveMode.Append)
      .csv(enrichPath)

    val sourceDFWithoutLkey = sourceDFFiltered.filter(col("lkey_web") === "Unknown" || col("lkey_web") === "NoMatch")
    val sourceMMEJoinedDF = TransactionDFOperations.joinWithMME(sourceDFWithoutLkey, locationDF, hdfsPartitions)
    val transWithMMELkeyOtherTables = TransactionDFOperations.joinForLookUps(sourceMMEJoinedDF, magnetDF, deviceDBDF, cspDF, radiusSRCDF)
    val transWithMMELkeyOtherTablesExpanded = TransactionDFOperations.sourceColumnSplit(spark, transWithMMELkeyOtherTables, "WEB")
    val transWithMMELkeyOtherTablesExpandedFinal = TransactionDFOperations.getFinalDF(transWithMMELkeyOtherTablesExpanded)
    transWithMMELkeyOtherTablesExpandedFinal.write.partitionBy("dt", "hour", "loc", "csp")
      .option("codec", "com.hadoop.compression.lzo.LzopCodec")
      .option("delimiter", "\t")
      .mode(SaveMode.Append)
      .csv(enrichPath)

    val HbaseConf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(spark.sparkContext, HbaseConf)
    val keysDF = sourceDFFiltered.select(col("userid_web_seq"))
    keysDF.show(20, false)
    val webBothDFsRDD = keysDF.map(row => row.getAs[String]("userid_web_seq").getBytes).rdd
    hbaseContext.bulkDelete[Array[Byte]](webBothDFsRDD, TableName.valueOf(transactionTable), deleteRecord => new Delete
    (deleteRecord), 4)

    //Update weblogs_batch_processed_ts with weblogs_stream_processed_ts

    val updatedCntlDF = controlDF.withColumn("weblogs_batch_processed_ts", lit(streamProccessedTimeVal))
      .withColumn("batch_job_status", lit("Completed"))
    Utils.updateHbaseColumn(controlCatalog, updatedCntlDF)

    spark.close()
  }
}
