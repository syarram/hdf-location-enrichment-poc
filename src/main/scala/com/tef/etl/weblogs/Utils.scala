package com.tef.etl.weblogs

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.spark.sql.execution.datasources.hbase.HBaseRelation
import org.apache.spark.sql.functions.{col, date_format, when}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Calendar
import java.time.format.DateTimeFormatter

object Utils {

  val logger = LoggerFactory.getLogger(Utils.getClass)
  /**
   * This method reads LZO formatted csv and returns dataframe.
   * @param spark
   * @param path
   * @param delimiter
   * @param schema
   * @return
   */
  def readLZO(spark:SparkSession,path:String,delimiter:String,schema:StructType):DataFrame={
    spark.sparkContext.hadoopConfiguration.set("io.compression.codecs", "com.hadoop.compression.lzo.LzopCodec")
    spark.read.format("csv")
      .option("delimiter",delimiter)
      .schema(schema)
      .load(path)
  }

  /**
   * This method returns partition location for given date,
   * if not exist then returns previous day partition location.
   * @param fs
   * @param loc
   * @param oDate
   * @return
   */
  def getLastPartition(fs:FileSystem, loc:String, oDate:String): String ={
    try{
      val partitionList = fs.listStatus(new Path(loc+"="+oDate)).map(p => p.getPath.toString)
      val partitionSize = partitionList.size
      if(partitionSize>=1) partitionList(partitionSize-1)
      else getLastPartition(fs,loc,previousDay(oDate))
    }catch{
      case e:FileNotFoundException =>{
        getLastPartition(fs,loc,previousDay(oDate))
      }
      case e:Exception =>{
        throw e
      }
    }
  }

  /**
   * This method gets latest avaible partition with in given maximum date range
   * @param fs
   * @param loc
   * @param oDate
   * @param maxOdate
   * @return
   */
  def getLastPartition(fs:FileSystem, loc:String, oDate:String, maxOdate:String):String ={
    if(isDateBefore(oDate, maxOdate)) "NotFound"
    else {
      try{
        val partitionList = fs.listStatus(new Path(loc+"="+oDate)).map(p => p.getPath.toString)
        val partitionSize = partitionList.size
        if(partitionSize>=1) loc+"="+oDate
        else  getLastPartition(fs,loc,getAmendedDate(oDate,-1),maxOdate)
      }catch{
        case e:FileNotFoundException =>{
          getLastPartition(fs,loc,getAmendedDate(oDate,-1),maxOdate)
        }
        case e:Exception =>{
          throw e
        }
      }
    }

  }

  /**
   * This Method takes date string and returns previous date to the input.
   * @param strDate
   * @return
   */
  def previousDay(strDate:String):String={
    val date = new SimpleDateFormat("yyyyMMdd")
    val currentDate =  date.parse(strDate)
    val cal = Calendar.getInstance()
      cal.setTime(currentDate)
    cal.add(Calendar.DATE,-1)
    date.format (cal.getTime)
  }

  /**
   * This method checks if date1 is before date2
   * @param date1
   * @param date2
   * @return
   */
  def isDateBefore(date1:String,date2:String):Boolean={
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val newDate = LocalDate.parse(date1,formatter)
    val maxDate = LocalDate.parse(date2,formatter)
    newDate.isBefore(maxDate)
  }

  /**
   * This method returns incremented/decremented date based on input
   * @param strDate
   * @param incrementBy
   * @return
   */
  def getAmendedDate(strDate:String,incrementBy:Int):String={
    val date = new SimpleDateFormat("yyyyMMdd")
    val currentDate =  date.parse(strDate)
    val cal = Calendar.getInstance()
    cal.setTime(currentDate)
    cal.add(Calendar.DATE,incrementBy)
    date.format (cal.getTime)
  }

  /**
   * This methods writes dataframe of specific tablecatalog to hbase.
   * @param catalog
   * @param controlDF
   */
  def updateHbaseColumn(catalog:String,controlDF:DataFrame): Unit ={
    controlDF.
      write.
      options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "10"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  /**
   * This method reads hbase table for a specific tablecatalog and returns dataframe.
   * @param format
   * @param catalog
   * @param spark
   * @return
   */
  def reader(format: String, catalog: String) (implicit spark: SparkSession) =
  {
    spark
      .read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format(format)
      .load()
  }

  /**
   * This method returns column value of a specific dataframe.
   * @param df
   * @param colName
   * @param spark
   * @return
   */
  def colValFromDF(df :DataFrame, colName: String)(implicit spark: SparkSession) : String = {
    val maxValue = df.select(colName).collectAsList().get(0).getString(0)
    maxValue
  }

  /**
   * This method reads hbase table for specific tablecatalog
   * then filters on min and max timestamp and returns dataframe.
   * @param format
   * @param catalog
   * @param min
   * @param max
   * @param spark
   * @return
   */
  def hbaseTimestampReader(format: String, catalog: String, min: String, max: String)(implicit spark: SparkSession) =
  {
    spark
      .read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .option(HBaseRelation.MIN_STAMP,min)
      .option(HBaseRelation.MAX_STAMP,max)
      .format(format)
      .load()
  }

  /**
   * This method writes dataframe to CSV file with LZO compression
   * @param df
   * @param enrichPath
   */
  def writeLZOCSV(df:DataFrame, path:String): Unit ={
    df.write.partitionBy("dt", "hour", "loc", "csp")
      .option("codec", "com.hadoop.compression.lzo.LzopCodec")
      .option("delimiter", "\t")
      .mode(SaveMode.Append)
      .csv(path)
  }

  def writeErichedData(df:DataFrame,enrichPath:String,errorPath:String): Unit ={
    val cachedDf = df.cache()
    if("none".equalsIgnoreCase(errorPath)){
      writeLZOCSV(cachedDf.drop("yr"),enrichPath)
    }else {
      import org.apache.spark.sql.functions._
      val enrichDF = cachedDf.filter(col("yr") > 2020 && col("yr") < 2300).drop("yr")
      writeLZOCSV(enrichDF,enrichPath)

      val errorDF = cachedDf.filter(col("yr") <= 2020 || col("yr") >= 2300)
        .drop("yr")
        .withColumn("dt",date_format(current_date,"yyyyMMdd"))
        .withColumn("hour",date_format(current_timestamp,"HH"))
      writeLZOCSV(errorDF,errorPath)
    }
  }
}
