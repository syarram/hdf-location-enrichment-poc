package com.tef.etl.SparkFuncs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.sql.execution.datasources.hbase.HBaseRegion
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row

/**
 * @author Mohamed Bilal S
 *
 */

object SparkUtilsOld {
  
  def dirExist(path: String)(implicit spark: SparkSession) = {
    val hadoopfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val p = new Path(path)
    hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isDirectory()
  }
  
  def hdfsInput(feed_path: String, min: String, mid: String, max: String)(implicit spark: SparkSession)={
    val min_dt_hr = date1(min)
    val max_dt_hr = date1(max)
    val mid_dt_hr = date1(mid)
    val feed_path1 = s"${feed_path}/dt=${min_dt_hr.split(",")(0)}/hour=${min_dt_hr.split(",")(1)}"
    val feed_path2 = s"${feed_path}/dt=${mid_dt_hr.split(",")(0)}/hour=${mid_dt_hr.split(",")(1)}"
    val feed_path3 = s"${feed_path}/dt=${max_dt_hr.split(",")(0)}/hour=${max_dt_hr.split(",")(1)}"
    val paths = List(feed_path1,feed_path2,feed_path3)
    paths.filter(p=>dirExist(p))    
    
  }
  
  def emptyDF()(implicit spark: SparkSession)={
    val emptySchema = StructType(Seq())
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
                emptySchema)
  }
  
  def hdfs_read(input_list: List[String])(implicit spark: SparkSession)={
      
    spark
    .read
    .orc(input_list:_*) 
  }
  
  def reader(format: String, catalog: String) (implicit spark: SparkSession) =
  {
    spark
    .read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .format(format)
    .load()
  }
  
      
  def webReader(format: String, catalog: String, min: String, max: String)(implicit spark: SparkSession) =
  {
    spark
    .read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .option(HBaseRelation.MIN_STAMP,min)
    .option(HBaseRelation.MAX_STAMP,max)
    .format(format)
    .load()
  }
  
  def hbaseWriter(outupd: org.apache.spark.sql.DataFrame, format: String, catalog: String) = {
    outupd
    .write
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .option(HBaseTableCatalog.newTable, "5")
    .format(format)
    .save()
  }
  
  def hdfsWriter(lkeyupd: org.apache.spark.sql.DataFrame, format: String, path: String, partitions: Int) = {
    lkeyupd
    .coalesce(partitions)
    .write
    .partitionBy("dt","hour")
    .format(format)
    .option("compression", "zlib")
    .mode(SaveMode.Append)
    .save(path)
    
  }
  
  def date1(dthr: String) = {
    val df:SimpleDateFormat = new SimpleDateFormat("YYYYMMdd,HH")
    df.format(dthr.toLong)
  }
  
  def lkeyUpdate(webdf: org.apache.spark.sql.DataFrame, hdfsdf: org.apache.spark.sql.DataFrame, mme_df: org.apache.spark.sql.DataFrame, max: String, hdfs_list: List[String] ) = {
    
    val dthour = date1(max)
    val dt = dthour.split(",")(0)
    val hour = dthour.split(",")(1)
    var null_df = nullProcess(webdf)
    
    if(hdfs_list.size > 0)
    {
     null_df = uniqueJoin(null_df, hdfsdf)
    }
    
    null_df
    .join(mme_df, null_df("userid_web") === mme_df("userid_mme") && mme_df("time_mme") <= null_df("time_web"),"left_outer")
    .orderBy(desc("time_mme"))
    .dropDuplicates(Array("time_web","userid_web_seq"))
    //.withColumn("lkey_web", mme_df("lkey_mme"))
    //.withColumn("lkey_web", when(mme_df("lkey_mme").isNull,"NotFound").otherwise(mme_df("lkey_mme")))
    .withColumn("lkey_web", when(mme_df("lkey_mme").equalTo("Unknown"),"NotFound").otherwise(mme_df("lkey_mme")))
    .drop("userid_mme_seq","userid_mme","time_mme","lkey_mme")
    .withColumn("dt", lit(dt)).withColumn("hour", lit(hour))
    
  }
  
  def notNullWrite(webdf: org.apache.spark.sql.DataFrame, hdfsdf: org.apache.spark.sql.DataFrame, max: String, hdfs_list: List[String])={
    val dthour = date1(max)
    val dt = dthour.split(",")(0)
    val hour = dthour.split(",")(1)
    var not_null_df = notNullProcess(webdf)
    
    if(hdfs_list.size > 0)
    {
    not_null_df = uniqueJoin(not_null_df, hdfsdf)
    }
   
    not_null_df
    .withColumn("dt", lit(dt)).withColumn("hour", lit(hour))                 
  }
  
  
  def notNullProcess(webdf: org.apache.spark.sql.DataFrame) = {
    //webdf.filter(webdf("lkey_web").notEqual("Unknown") && webdf("lkey_web").notEqual("NoLocation"))
    webdf.filter(webdf("lkey_web").equalTo("NoMatch") || webdf("lkey_web").equalTo("NoLocation"))
  }
  
 def nullProcess(webdf: org.apache.spark.sql.DataFrame) = {
    webdf.filter(webdf("lkey_web").equalTo("Unknown"))
  }
 
 def uniqueJoin(webdf: org.apache.spark.sql.DataFrame, hdfsdf: org.apache.spark.sql.DataFrame)={
   webdf
    .join(hdfsdf,webdf("userid_web_seq") === hdfsdf("userid_web_seq"),"left_anti")
 }
   
  def max_time(timedf : org.apache.spark.sql.DataFrame)(implicit spark: SparkSession) = {
    val max_time_int = max_time_actual(timedf)
    max_time_int.substring(1, max_time_int.length - 1)
  }
  
  def max_time_actual(timedf: org.apache.spark.sql.DataFrame) = {
    timedf.select("ts_time").collect().mkString
  }
  
  def res_time(maxtime: String, typ: String) = {
      var time = 0L
      typ match{
        case "min" => time = maxtime.toLong - 7200000 
        case "mid" => time = maxtime.toLong - 3600000
        case "max_null" => time = maxtime.toLong - 1199999
        case "max_not_null" => time = maxtime.toLong + 1
        case _ => time
      }
      time.toString()
  }
  
}