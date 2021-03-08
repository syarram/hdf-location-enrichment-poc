package com.tef.etl.main
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.sql.execution.datasources.hbase.HBaseRegion
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import com.tef.etl.SparkFuncs._
import com.tef.etl.catalogs._
import org.apache.spark.sql.expressions.Window

object test extends SparkSessionTrait{
  def main(args:Array[String]): Unit ={
    
    
    val format = "org.apache.spark.sql.execution.datasources.hbase"
    val hdfs_format = "orc"
    val hdfs_path = args(0)
    val partitions = args(1).toInt
    val mme_table_name = "\"" + args(2) + "\""
    val web_table_name = "\"" + args(3) + "\""
    val min_time_ckpt_table = "\"" + args(4) + "\""
    val min_time_ckpt_cf = "\"" + args(5) + "\""
    val min_time_ckpt_col = "\"" + args(6) + "\""
    val max_time_ckpt_table = "\"" + args(7) + "\""
    val max_time_ckpt_cf = "\"" + args(8) + "\""
    val max_time_ckpt_col = "\"" + args(9) + "\""
    val min_time_reduce = args(10)
    val max_time_reduce = args(11)
    val hdfs_read_time = args(12)
    val hdfs_min_time_reduce = args(13)
    val hdfs_mid_time_reduce = args(14)
    
    val mintimeCatalog = HBaseCatalogs
                        .timeStampCatalog(min_time_ckpt_table,min_time_ckpt_cf,min_time_ckpt_col)
    val maxtimeCatalog = HBaseCatalogs
                        .timeStampCatalog(max_time_ckpt_table,max_time_ckpt_cf,max_time_ckpt_col)
    
    val max_time_df = SparkUtils.reader(format, maxtimeCatalog)(spark)
                        
    import spark.implicits._
    if(max_time_df.filter($"ts_time".isNull).count >= 1)
    {
      spark.stop()
    }
            
    val mmeCatalog = HBaseCatalogs.mmecatalog(mme_table_name)
    val webCatalog = HBaseCatalogs.stagewebcatalog(web_table_name)
    
    /*Timestamp variation to read from HBase*/
    val max = SparkUtils.max_time(max_time_df)
    val min = SparkUtils.res_time(max,"min",min_time_reduce)
    val max_null = SparkUtils.res_time(max,"max_null",max_time_reduce)
    val max_not_null = SparkUtils.res_time(max,"max_not_null","1")
    
    /*timestamp variation to read from HDFS*/
    val hdfs_min = SparkUtils.res_time(hdfs_read_time,"min",hdfs_min_time_reduce)
    val hdfs_mid = SparkUtils.res_time(hdfs_read_time,"mid",hdfs_mid_time_reduce)
    val hdfs_max = SparkUtils.res_time(hdfs_read_time,"max_not_null","1")
        
    val hdfs_input_list = SparkUtils.hdfsInput(hdfs_path,hdfs_min,hdfs_mid,hdfs_max)
        
    var hdfs_read = SparkUtils.emptyDF()(spark)
        
    if(hdfs_input_list.size > 0)
    {
      hdfs_read = SparkUtils.hdfs_read(hdfs_input_list)(spark)
    }
    
    def joinFunction1(webdf: org.apache.spark.sql.DataFrame,mme_df: org.apache.spark.sql.DataFrame)={
     /* val windowSpec = Window.partitionBy("userid_mme_seq").orderBy(desc("time_mme"))
   val joiner = webdf
    .join(mme_df, webdf("userid_web") === mme_df("userid_mme") && mme_df("time_mme") <= webdf("time_web"),"left_outer")
    .repartition(webdf("partition_web"))
    .withColumn("drank", dense_rank().over(windowSpec))
    .filter(col("drank") === 1)
    .withColumn("lkey_web", when(mme_df("lkey_mme").isNull,"NotFound").otherwise(mme_df("lkey_mme")))
    .drop("userid_mme_seq","userid_mme","lkey_mme")*/
    val joiner = webdf
    .join(mme_df, webdf("userid_web") === mme_df("userid_mme") && mme_df("time_mme") <= webdf("time_web"),"left_outer")
    .repartition(webdf("partition_web"))
    .sortWithinPartitions(desc("time_mme"))
    .dropDuplicates(Array("time_web","userid_web_seq"))
    .withColumn("lkey_web", when(mme_df("lkey_mme").isNull,"NotFound").otherwise(mme_df("lkey_mme")))
    .drop("userid_mme_seq","userid_mme","lkey_mme")
    
    joiner
    .withColumn("dt", SparkUtils.part_dt(col("time_web")))
    .withColumn("hour", SparkUtils.part_hour(col("time_web")))
 }
        
        
    val mme_df = SparkUtils.reader(format, mmeCatalog)(spark)
    
    val web_non_df = SparkUtils.webReader(format, webCatalog, min, max_not_null)(spark)
    val two_hour_list = SparkUtils.twoHourList(web_non_df, hdfs_read, max,hdfs_input_list)
    val df3 = two_hour_list(0).
              filter(
              (col("userid_web") === "++Bufe3+sGtUWbMt/YUU5lMbwkz4FIm/+2e/jMlSLFY=" &&
              col("time_web") === "1588581761290") ||
              (col("userid_web") === "++5FlkEkLFCUnCPSCQ0gAG15pvvFrnoIn5qfIxxha1c=" &&
              col("time_web") === "1588582522387")
              
              )
    
    
    
    
    val lkey_upd_join = joinFunction1(df3, mme_df)
    
    val hdfs_lkeyupd_write = SparkUtils.hdfsWriter(lkey_upd_join, hdfs_format, hdfs_path, partitions)
    
  }
}