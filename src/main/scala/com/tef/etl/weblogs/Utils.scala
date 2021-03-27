package com.tef.etl.weblogs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.StructType

import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object Utils {

  def readLZO(spark:SparkSession,path:String,delimiter:String,schema:StructType):DataFrame={
    spark.sparkContext.hadoopConfiguration.set("io.compression.codecs", "com.hadoop.compression.lzo.LzopCodec")
    spark.read.format("csv")
      .option("delimiter",delimiter)
      .schema(schema)
      .load(path)
  }

  def getLastPartition(fs:FileSystem, loc:String, oDate:String): String ={
    try{
      val partitionList = fs.listStatus(new Path(loc+oDate)).map(p => p.getPath.toString)
      val partitionSize = partitionList.size
      if(partitionSize>=1) partitionList(partitionSize-1) else getLastPartition(fs,loc,previousDay(oDate))
    }catch{
      case e:FileNotFoundException =>{
        getLastPartition(fs,loc,previousDay(oDate))
      }
      case e:Exception =>{
        throw e
      }
    }
  }

  def previousDay(strDate:String):String={
    val date = new SimpleDateFormat("yyyyMMdd")
    val currentDate =  date.parse(strDate)
    val cal = Calendar.getInstance()
      cal.setTime(currentDate)
    cal.add(Calendar.DATE,-1)
    date.format (cal.getTime)
  }

}
