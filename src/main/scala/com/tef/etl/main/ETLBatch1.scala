/*

package com.tef.etl.main

/* Author name : Mohamed Bilal S */

import com.tef.etl.SparkFuncs._
import com.tef.etl.catalogs._

object ETLBatch1 extends SparkSessionTrait {
  def main(args: Array[String]): Unit ={ 
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
        
        
    val mme_df = SparkUtils.reader(format, mmeCatalog)(spark)
    
    val web_non_df = SparkUtils.webReader(format, webCatalog, min, max_not_null)(spark)
    val two_hour_list = SparkUtils.twoHourList(web_non_df, hdfs_read, max,hdfs_input_list)
    val df3 = two_hour_list(0)
    val df4 = two_hour_list(1)
    
    val web_null_df = SparkUtils.webReader(format, webCatalog, min, max_null)(spark)
    val one_forty_list = SparkUtils.oneFortyList(web_null_df, hdfs_read, mme_df,max,hdfs_input_list)
    val df1 = one_forty_list(0)
    val df2 = one_forty_list(1)
    
    
    val uniondf_lkey_upd = SparkUtils.unionDF(df1,df3,hdfs_read,hdfs_input_list,max)
    val lkey_upd_join = SparkUtils.joinFunction(uniondf_lkey_upd, mme_df)
    
    val uniondf_lkey_no_upd = SparkUtils.unionDF(df2,df4,hdfs_read,hdfs_input_list,max)
    
    val hdfs_no_lkeyupd_write = SparkUtils.hdfsWriter(uniondf_lkey_no_upd, hdfs_format, hdfs_path, partitions)
    val hdfs_lkeyupd_write =    SparkUtils.hdfsWriter(lkey_upd_join, hdfs_format, hdfs_path, partitions)
    
    val time_writer = SparkUtils.hbaseWriter(max_time_df, format, mintimeCatalog)
    
    

    
  }
}*/
