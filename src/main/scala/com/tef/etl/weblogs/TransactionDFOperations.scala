package com.tef.etl.weblogs

import com.tef.etl.definitions.WeblogDef
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.split

object TransactionDFOperations {

  def sourceColumnSplit(spark:SparkSession, df: DataFrame, fileType:String="MME"): DataFrame = {
    val colNames = WeblogDef.webColumnNames
    import spark.implicits._
    val df1 = df.withColumn("descArray", split($"nonlkey_cols","\\|"))
    df1.select(col("descArray") +: (0 until colNames.length)
      .map(x=> col("descArray")(x).alias(colNames(x))
    ):_*).drop("nonlkey_cols").drop("descArray")
  }
}
