package com.tef.etl.main

import com.tef.etl.SparkFuncs.{SparkSessionTrait, SparkUtils}
import com.tef.etl.SparkFuncs.SparkUtils.{part_dt, part_hour}
import com.tef.etl.catalogs.HBaseCatalogs
import com.tef.etl.weblogs.TransactionDFOperations
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Delete, HTable, Put}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.slf4j.{Logger, LoggerFactory}


//import org.apache.hadoop.hbase.client.TableDescriptor
//import org.apache.hadoop.hbase.client.TableDescriptor
import org.apache.hadoop.hbase.spark.HBaseRelation
import org.apache.hadoop.hbase.HBaseIOException
import org.apache.spark.sql.execution.datasources.hbase
import org.json4s.jackson.JsonMethods
//import org.apache.hadoop.hbase.AuthUtil.loginClient


import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog


object WebIpfrBatchEnrich extends SparkSessionTrait {

  def main(args: Array[String]): Unit = {


    val format = "org.apache.spark.sql.execution.datasources.hbase"
    val locationTable = "\"" + args(0) + "\""
    val transactionTable = "\"" + args(1) + "\""
    val transactionTableConnector = args(1)
    val webIpfrEnrichControl = "\"" + args(2) + "\""
    val webIpfrEnrichControlConnector = args(2)
    val errorLogging = args(3)
    val partitions = args(4).toInt
    val hdfsPath = args(5)
    val hdfsPartitions = args(6).toInt
    val transactionTableKeys = "\"" + args(7) + "\""
    val transactionTableKeysConnector = args(7)
    val compressionFormat = args(8)
    val hdfsFormat = args(9)
    val deleteBatchSize = args(10).toInt

    val logger = LoggerFactory.getLogger(WebIpfrBatchEnrich.getClass)


    import spark.implicits._
    spark.sparkContext.setLogLevel(errorLogging)


    // Delete records from source Table if the previous run didnt finish successfully
    val transactionKeys = HBaseCatalogs.stageTrnsactionKeys(transactionTableKeys)
    val stageKeys = SparkUtils.reader(format, transactionKeys)(spark)
    val mmeCatalog = HBaseCatalogs.mmecatalog(locationTable)
    val webCatalog = HBaseCatalogs.stagewebcatalog(transactionTable)
    val controlCatalog = HBaseCatalogs.webipfr_enrich_control(webIpfrEnrichControl)
    tempStageKeysDelete(transactionTableKeysConnector, stageKeys)
    val sourceDF = (SparkUtils.reader(format, webCatalog)(spark))//.filter(col("userid_web").isNotNull)
    val expandedDF = TransactionDFOperations.sourceColumnSplit(spark,sourceDF,"WEB")
    expandedDF.show(100,false)

    //sourceDF.show(100,false)

    //  val locationDF = SparkUtils.reader(format, mmeCatalog)(spark)
    //  locationDF.show(10,false)

    //example output live file
    // /data/web/dt=20210303/hour=22/loc=CRX1/csp=O2/FlumeData_10.42.146.255.1614812755448.lzo
    sourceDF.repartition(hdfsPartitions)
      .write
      .format(hdfsFormat)    // ***Issue here.....working fine for parquet
      .option("compression", compressionFormat)
      //    .mode(SaveMode.Append)
      .mode(SaveMode.Overwrite)
      .save(hdfsPath)



    val onlyKeys = sourceDF//.limit(2000000)
    val conf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(spark.sparkContext, conf)
    val webBothDFsRDD = onlyKeys.select(col("userid_web_seq")).map(row => row.getAs[String]("userid_web_seq").getBytes).rdd
  //  hbaseContext.bulkDelete[Array[Byte]](webBothDFsRDD, TableName.valueOf(transactionTableConnector), deleteRecord => new Delete(deleteRecord),deleteBatchSize)




 //--    println("****************************************** Source count: "+sourceDF.count()+", Location Count: "+locationDF.count)

  /*  val webIpfrEnrichControlDF = SparkUtils.reader(format, controlCatalog)(spark)
    val streamProccessedTimeVal = SparkUtils.colValFromDF(webIpfrEnrichControlDF, "weblogs_stream_processed_ts")(spark)

    logger.info("***************************************"+sourceDF.count)
    logger.error("***************************************"+sourceDF.count)

    // Select Records based on Stream processed TimeStamp, records after streamprocessed timestamp should not be touched.
    val sourceTSFiltered = sourceDF.filter(col("time_web") <= streamProccessedTimeVal ).cache  //&& (col("lkey_web") === "unknown" || col("lkey_web") === "NoMatch")
    val sourceColList = sourceDF.schema.fieldNames



    val sourceDFWithLkey = sourceTSFiltered.filter(col("lkey_web").notEqual("unknown") && col("lkey_web").notEqual("NoMatch"))
    val sourceDFWithoutLkey = sourceTSFiltered.filter(col("lkey_web") === "unknown" || col("lkey_web") === "NoMatch")//.cache
*/
 /*   val webJoinedDF =
      sourceDFWithoutLkey
        .join(locationDF, sourceDFWithoutLkey("userid_web") === locationDF("userid_mme") && locationDF("time_mme") <= sourceDFWithoutLkey("time_web") && sourceDFWithoutLkey("partition_web") === locationDF("partition_mme"),"left_outer")
        .drop("lkey_web").withColumn("lkey_web", when(locationDF("lkey_mme").isNull,"NotFound").otherwise(locationDF("lkey_mme")))
        .repartition(hdfsPartitions, col("userid_web"))
        .sortWithinPartitions(col("userid_web_seq"),desc("time_mme"))
        .dropDuplicates(Array("time_web","userid_web_seq"))
        .select(sourceColList.head, sourceColList.tail:_*)//.drop("lkey_web")


    val webBothDFs = webJoinedDF.union(sourceDFWithLkey)
      .withColumn("dt", part_dt(col("time_web")))
      .withColumn("hour", part_hour(col("time_web")))//.cache

    val webBothDFsKeys = webBothDFs.select("userid_web_seq")
*/
//  webBothDFs.show(10,false)
 /*   webBothDFs
      .repartition(hdfsPartitions)
      .write.partitionBy("dt","hour","loc","csp")
      .format(hdfsFormat)    // ***Issue here.....working fine for parquet
      .option("compression", compressionFormat)
      .mode(SaveMode.Append)
      .save(hdfsPath)*/


// Deleting Data from Source Transaction Table
 /*     val conf = HBaseConfiguration.create()
      val hbaseContext = new HBaseContext(spark.sparkContext, conf)
      val webBothDFsRDD = webBothDFsKeys.select(col("userid_web_seq")).map(row => row.getAs[String]("userid_web_seq").getBytes).rdd
      hbaseContext.bulkDelete[Array[Byte]](webBothDFsRDD, TableName.valueOf(transactionTableConnector), deleteRecord => new Delete(deleteRecord),4)
*/
// updating control table with timestamp
    /*    val config = HBaseConfiguration.create
        // instantiate HTable class
        val controlTable = new HTable(config, webIpfrEnrichControlConnector)
        // instantiate Put class
        var put = new Put(Bytes.toBytes("1000000"))
        // add values using add() method
        put.add(Bytes.toBytes("cfEnrich"), Bytes.toBytes("weblogs_batch_processed_ts"), Bytes.toBytes(streamProccessedTimeVal))
        controlTable.put(put)*/





// add functionality of deleting data from stage_weblogs


    /*lkeyupd
      .repartition(partitions)
      .write
      .partitionBy("dt","hour")
      .format(format)
      .option("compression", "zlib")
      .mode(SaveMode.Append)
      .save(path)*/

    //spark.read.format("namesAndFavColors10").show(10,false)
   /* spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header",false).csv("namesAndFavColors10")
      .show(false)*/

  //    Thread.sleep(1000000)

//--    println("**************************** SourceCount: "+  sourceDF.count()  +", Filtered Table Count: "+ sourceTSFiltered.count+", **** SourceWithLkey: "+ sourceDFWithLkey.count() +", **** SourceWithOutLkey: "+ sourceDFWithoutLkey.count() +", Control Table Count: "+webIpfrEnrichControlDF.count+", ************webBothDFs count: "+webBothDFs.count()+", streamProccessedTimeVal: "+"1597494981231")
    spark.close()
  }

  private def tempStageKeysDelete(transactionTableKeysConnector: String, stageKeys: DataFrame) = {
    import spark.implicits._
    if (stageKeys.count > 0) {
      println("******************Delete didnt happen successfully in previour run - deleting " + stageKeys.count + " records here")

      val conf = HBaseConfiguration.create()
      val hbaseContext = new HBaseContext(spark.sparkContext, conf)
      val stageKeysRDD = stageKeys.select(col("userid_web_seq")).map(row => row.getAs[String]("userid_web_seq").getBytes).rdd
      hbaseContext.bulkDelete[Array[Byte]](stageKeysRDD, TableName.valueOf(transactionTableKeysConnector), deleteRecord => new Delete(deleteRecord), 4)
    }
  }
}
