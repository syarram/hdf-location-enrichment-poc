/*
package com.tef.etl.main

/* Author name : Mohamed Bilal S */

import com.tef.etl.SparkFuncs._
import com.tef.etl.catalogs._

object ETLBatch extends SparkSessionTrait {
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
    
    val mintimeCatalog = HBaseCatalogs
                        .timeStampCatalog(min_time_ckpt_table,min_time_ckpt_cf,min_time_ckpt_col)
    val maxtimeCatalog = HBaseCatalogs
                        .timeStampCatalog(max_time_ckpt_table,max_time_ckpt_cf,max_time_ckpt_col)
    
    val max_time_df = SparkUtilsOld.reader(format, maxtimeCatalog)(spark)
                        
    import spark.implicits._
    if(max_time_df.filter($"ts_time".isNull).count >= 1)
    {
      spark.stop()
    }
            
    val mmeCatalog = HBaseCatalogs.mmecatalog(mme_table_name)
    val webCatalog = HBaseCatalogs.stagewebcatalog(web_table_name)
    
    val max = SparkUtilsOld.max_time(max_time_df)
    val min = SparkUtilsOld.res_time(max,"min")
    val mid = SparkUtilsOld.res_time(max,"mid")
    val max_null = SparkUtilsOld.res_time(max,"max_null")
    val max_not_null = SparkUtilsOld.res_time(max,"max_not_null")
        
    val hdfs_input_list = SparkUtilsOld.hdfsInput(hdfs_path,min,mid,max)
        
    var hdfs_read = SparkUtilsOld.emptyDF()(spark)
        
    if(hdfs_input_list.size > 0)
    {
      hdfs_read = SparkUtilsOld.hdfs_read(hdfs_input_list)(spark)
    }
        
        
    val mme_df = SparkUtilsOld.reader(format, mmeCatalog)(spark)
    
    val web_non_df = SparkUtilsOld.webReader(format, webCatalog, min, max_not_null)(spark)
    val not_null_process = SparkUtilsOld.notNullWrite(web_non_df, hdfs_read, max,hdfs_input_list)
    val hdfs_not_null_write = SparkUtilsOld.hdfsWriter(not_null_process, hdfs_format, hdfs_path, partitions)
    
    val web_null_df = SparkUtilsOld.webReader(format, webCatalog, min, max_null)(spark)
    val lkey_upd = SparkUtilsOld.lkeyUpdate(web_null_df, hdfs_read, mme_df,max,hdfs_input_list)
    val hdfs_null_write = SparkUtilsOld.hdfsWriter(lkey_upd, hdfs_format, hdfs_path, partitions)
    
    val time_writer = SparkUtilsOld.hbaseWriter(max_time_df, format, mintimeCatalog)
      
    
  }
}*/
