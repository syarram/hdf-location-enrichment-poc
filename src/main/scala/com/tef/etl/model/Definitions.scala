package com.tef.etl.model

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Definitions {
  val magnetSchema:StructType = StructType(Array(
  StructField("csr", StringType, nullable = true),
  StructField("cell_id", StringType, nullable = true),
  StructField("sector", StringType, nullable = true),
  StructField("mi_mycom_id", StringType, nullable = true),
  StructField("generation", StringType, nullable = true),
  StructField("wcel_id", StringType, nullable = true),
  StructField("fdd", StringType, nullable = true),
  StructField("model_name", StringType, nullable = true),
  StructField("site_name", StringType, nullable = true),
  StructField("site_type", StringType, nullable = true),
  StructField("cell_type", StringType, nullable = true),
  StructField("manufacturer", StringType, nullable = true),
  StructField("lacod", StringType, nullable = true),
  StructField("ne_id", StringType, nullable = true),
  StructField("msc", StringType, nullable = true),
  StructField("cell_carriers", StringType, nullable = true),
  StructField("cell_3g_carrier", StringType, nullable = true),
  StructField("cell_of_origin", StringType, nullable = true),
  StructField("zone_code", StringType, nullable = true),
  StructField("adr1", StringType, nullable = true),
  StructField("adr2", StringType, nullable = true),
  StructField("adr3", StringType, nullable = true),
  StructField("town", StringType, nullable = true),
  StructField("country", StringType, nullable = true),
  StructField("postcode", StringType, nullable = true),
  StructField("easting", StringType, nullable = true),
  StructField("northing", StringType, nullable = true),
  StructField("bcch", StringType, nullable = true),
  StructField("arfcn", StringType, nullable = true),
  StructField("trx_list", StringType, nullable = true),
  StructField("ncc", StringType, nullable = true),
  StructField("bcc", StringType, nullable = true),
  StructField("scc", StringType, nullable = true),
  StructField("sac", StringType, nullable = true),
  StructField("rac", StringType, nullable = true),
  StructField("tma", StringType, nullable = true),
  StructField("environment", StringType, nullable = true),
  StructField("kit_type", StringType, nullable = true),
  StructField("antenna_num", StringType, nullable = true),
  StructField("beam_width", StringType, nullable = true),
  StructField("ant_name", StringType, nullable = true),
  StructField("ant_height", StringType, nullable = true),
  StructField("ground_height", StringType, nullable = true),
  StructField("tilt", StringType, nullable = true),
  StructField("elec_tilt", StringType, nullable = true),
  StructField("azimuth", StringType, nullable = true),
  StructField("power", StringType, nullable = true),
  StructField("losses", StringType, nullable = true),
  StructField("area", StringType, nullable = true),
  StructField("region", StringType, nullable = true),
  StructField("bis_date", StringType, nullable = true),
  StructField("site_generation_type", StringType, nullable = true),
  StructField("cell_owner", StringType, nullable = true),
  StructField("jv_id", StringType, nullable = true),
  StructField("enodeb_id", StringType, nullable = true),
  StructField("pci", StringType, nullable = true),
  StructField("tac", StringType, nullable = true),
  StructField("rsi", StringType, nullable = true),
  StructField("ura", StringType, nullable = true),
  StructField("enodebname", StringType, nullable = true),
  StructField("databuild_dttm", StringType, nullable = true),
  StructField("lkey", StringType, nullable = true),
  StructField("latitude", StringType, nullable = true),
  StructField("longitude", StringType, nullable = true)))

  val magnetSchema:StructType = StructType(Array())
}
