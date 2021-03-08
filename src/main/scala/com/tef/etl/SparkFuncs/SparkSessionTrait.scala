package com.tef.etl.SparkFuncs

import org.apache.spark.sql.SparkSession

/**
 * @author Mohamed Bilal
 *
 */

trait SparkSessionTrait {
  implicit  val spark = SparkSession
    .builder()
    .appName("HBASE WEB")
    .getOrCreate()

}