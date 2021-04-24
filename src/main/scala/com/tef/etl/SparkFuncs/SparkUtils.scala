package com.tef.etl.SparkFuncs

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.text.SimpleDateFormat

/**
 * @author Mohamed Bilal S
 *
 */

object SparkUtils {
  
  /*Check if a particular directory exists in HDFS*/
  def dirExist(path: String)(implicit spark: SparkSession) = {
    val hadoopfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val p = new Path(path)
    hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isDirectory()
  }
  
  /*Return the list of direcotries available in HDFS output for the given timestamp*/
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
  
  /*Create an empty dataframe*/
  def emptyDF()(implicit spark: SparkSession)={
    val emptySchema = StructType(Seq())
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row],
                emptySchema)
  }
  
  /*Read the output HDFS record*/
  def hdfs_read(input_list: List[String])(implicit spark: SparkSession)={
      
    spark
    .read
    .orc(input_list:_*) 
  }
  
  /* Common function to read from full HBase table*/
  def reader(format: String, catalog: String) (implicit spark: SparkSession) =
  {
    spark
    .read
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .format(format)
    .load()
  }
  
    /* Common function to read from HBase table filtering based 
     *                   on min and max HBase timestamps*/  
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


  def mmeReader(format: String, catalog: String, min: String, max: String)(implicit spark: SparkSession) =
  {
    spark
      .read
      .option(HBaseTableCatalog.tableCatalog, catalog)
          .option(HBaseRelation.MIN_STAMP,min)
      .option(HBaseRelation.MAX_STAMP,max)
      .format(format)
      .load()
  }


  /*Common function to write to a HBase table
     *    In our module we use this to update the timestamp checkpoint table */
  def hbaseWriter(outupd: org.apache.spark.sql.DataFrame, format: String, catalog: String) = {
    outupd
    .write
    .option(HBaseTableCatalog.tableCatalog, catalog)
    .option(HBaseTableCatalog.newTable, "5")
    .format(format)
    .save()
  }
  
  /* This function writes the output dataframe to HDFS based on dt and hr partition*/
  def hdfsWriter(lkeyupd: org.apache.spark.sql.DataFrame, format: String, path: String, partitions: Int) = {
    lkeyupd
    .repartition(partitions)
    .write
    .partitionBy("dt","hour")
    .format(format)
    .option("compression", "zlib")
    .mode(SaveMode.Append)
    .save(path)

    println("************************** Inside HDFS Writer")
  }
  
  /*Convert the timestamp and extract the date in YYYYMMdd format and hour in HH format*/
  def date1(dthr: String) = {
    val df:SimpleDateFormat = new SimpleDateFormat("YYYYMMdd,HH")
    df.format(dthr.toLong)
  }
  
  /*Get the records for one hour forty minutes
   * Filter the records with value Unknown and with lkeys populated
   * Return the dataframes as list*/
  def oneFortyList(webdf: org.apache.spark.sql.DataFrame, hdfsdf: org.apache.spark.sql.DataFrame, mme_df: org.apache.spark.sql.DataFrame, max: String, hdfs_list: List[String] ) = {
    
    
    val df1 = webdf.filter(webdf("lkey_web").equalTo("Unknown"))
    val df2 = webdf.filter(webdf("lkey_web").notEqual("Unknown") &&
              webdf("lkey_web").notEqual("NoMatch") &&
              webdf("lkey_web").notEqual("NoLocation"))
    List(df1,df2)
    
  }
  
  
  /*Get the records for one hour forty minutes
   * Filter the records with value NoMatch and NoLocation populated
   * Return the dataframes as list*/
  def twoHourList(webdf: org.apache.spark.sql.DataFrame, hdfsdf: org.apache.spark.sql.DataFrame, max: String, hdfs_list: List[String])={
    
    val df3 = webdf.filter(webdf("lkey_web").equalTo("NoMatch"))
    val df4 = webdf.filter(webdf("lkey_web").equalTo("NoLocation"))
    List(df3,df4)
         
  }
 
  /*MME Lookup and get the lkey for the userids using left outer join
   * Populate NotFound for the resultant records with no lkeys
   * Drop the mme related columns*/
 def joinFunction(webdf: org.apache.spark.sql.DataFrame,mme_df: org.apache.spark.sql.DataFrame)(implicit spark: SparkSession)={
   //println("*******************WithInJoinMethod"+webdf.count())
   //webdf.show(10,false)
   val webJoinedDF =
   webdf
    //.join(mme_df, webdf("userid_web") === mme_df("userid_mme") && mme_df("time_mme") <= webdf("time_web"),"left_outer")
   .join(mme_df, webdf("userid_web") === mme_df("userid_mme") && mme_df("time_mme") <= webdf("time_web") && webdf("partition_web") === mme_df("partition_mme"),"left_outer") 
   //.orderBy(desc("time_mme"))
    //.repartition(col("partition_web"))
   .repartition(1000, col("userid_web"))
    .sortWithinPartitions(col("userid_web_seq"),desc("time_mme"))
    .dropDuplicates(Array("time_web","userid_web_seq"))
    //.withColumn("lkey_web", when(mme_df("lkey_mme").equalTo("Unknown"),"NotFound").when(mme_df("lkey_mme").equalTo("NoMatch"),"NotFound").otherwise(mme_df("lkey_mme")))
  //  .withColumn("lkey_web", when(mme_df("lkey_mme").isNull,"NotFound").otherwise(mme_df("lkey_mme")))
   // .drop("userid_mme_seq","userid_mme","time_mme","lkey_mme","partition_mme")

   webJoinedDF.withColumn("lkey_web", when(mme_df("lkey_mme").isNull,"NotFound").otherwise(mme_df("lkey_mme")))
     .drop("userid_mme_seq","userid_mme","time_mme","lkey_mme","partition_mme")


 }
 
 /*Combine DF1 and DF3, DF2 and DF4
  * Add the partition columns to the dataframe - dt,hr*/
 def unionDF(df1:org.apache.spark.sql.DataFrame,df2:org.apache.spark.sql.DataFrame,
              hdfsdf: org.apache.spark.sql.DataFrame,hdfs_list: List[String],max: String) = {
      
   var uniondf = df1.union(df2)
   if(hdfs_list.size > 0)
    {
     uniondf = uniqueJoin(uniondf, hdfsdf)
    }
   
    uniondf
    .withColumn("dt", part_dt(col("time_web")))
    .withColumn("hour", part_hour(col("time_web")))
   
 }
 
 /*Perform left anti join to reject the duplicate records from processing again*/
 def uniqueJoin(webdf: org.apache.spark.sql.DataFrame, hdfsdf: org.apache.spark.sql.DataFrame)={
   webdf
    .join(hdfsdf,webdf("userid_web_seq") === hdfsdf("userid_web_seq"),"left_anti")
 }
   
 /*Read the max timestamp from HBase table and convert the dataframe column into string*/
  def max_time(timedf : org.apache.spark.sql.DataFrame)(implicit spark: SparkSession) = {
    val max_time_int = max_time_actual(timedf)
    max_time_int.substring(1, max_time_int.length - 1)
  }
  
  def max_time_actual(timedf: org.apache.spark.sql.DataFrame) = {
    timedf.select("ts_time").collect().mkString
  }
  
  /*Value generation for date partition using the srcTimeStamp column*/
  val part_dt = udf((col: String) => {
      val df:SimpleDateFormat = new SimpleDateFormat("YYYYMMdd")
      df.format(col.toLong)
      //df.format(col.toString())
    })
  
    /*Value generation for hr partition using the srcTimeStamp column*/
  val part_hour = udf((col: String) => {
      val df:SimpleDateFormat = new SimpleDateFormat("HH")
      df.format(col.toLong)
      //df.format(col.toString())
    })
  
    /* Get the min, mid, max timestamps for processing using the max timestamp from HBase table*/
  def res_time(maxtime: String, typ: String, time_to_red: String) = {
      var time = 0L
      typ match{
        case "min" => time = maxtime.toLong - time_to_red.toLong 
        case "mid" => time = maxtime.toLong - time_to_red.toLong
        case "max_null" => time = maxtime.toLong - time_to_red.toLong
        case "max_not_null" => time = maxtime.toLong + time_to_red.toLong
        case _ => time
      }
      time.toString()
  }

  /*def maxColValFromDF(df :DataFrame, colName: String) : String = {
    df.show(10,false)
    val maxValue = df.groupBy(col(colName)).agg(max((colName))).collect().toString()
    maxValue
  }
*/
  def colValFromDF(df :DataFrame, colName: String)(implicit spark: SparkSession) : String = {
    val maxValue = df.select(colName).collectAsList().get(0).getString(0)
    maxValue
  }

}