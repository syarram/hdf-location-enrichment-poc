package com.tef.etl.catalogs
/* @author Mohamed Bilal

 *
 */
object HBaseCatalogs {
  
  /* Schema build for Web table*/
  def stagewebcatalog(table_name: String) = s"""{
    "table":{"namespace":"default", "name":${table_name}},
    "rowkey":"key",
    "columns":{
      "userid_web_seq":{"cf":"rowkey", "col":"key", "type":"string"},
      "userid_web":{"cf":"cfNonLKey", "col":"userId", "type":"string"},
      "time_web":{"cf":"cfNonLKey", "col":"srcTimeStamp", "type":"string"},
      "lkey_web":{"cf":"cfLKey", "col":"lkey", "type":"string"},
      "partition_web":{"cf":"cfNonLKey", "col":"partitionNum", "type":"string"},
      "nonlkey_cols":{"cf":"cfNonLKey", "col":"nonlkey_cols", "type":"string"},
      "loc":{"cf":"cfNonLKey", "col":"loc", "type":"string"},
      "csp":{"cf":"cfNonLKey", "col":"csp", "type":"string"}
    }
  }""".stripMargin

  def cspCatalog(table_name:String): String = s"""{
    "table":{"namespace":"default", "name":${table_name}},
    "rowkey":"key",
    "columns":{
      "ip":{"cf":"rowkey", "col":"key", "type":"string"},
      "apnid":{"cf":"cfcspapn", "col":"APN", "type":"string"},
      "apn-name":{"cf":"cfcspapn", "col":"APNName", "type":"string"},
      "csp":{"cf":"cfcspapn", "col":"CSP", "type":"string"},
      "network":{"cf":"cfcspapn", "col":"Network", "type":"string"}
    }
  }""".stripMargin


  def stageTrnsactionKeys(table_name: String) = s"""{
    "table":{"namespace":"default", "name":${table_name}},
    "rowkey":"key",
    "columns":{
      "userid_web_seq":{"cf":"rowkey", "col":"key", "type":"string"}
    }
  }""".stripMargin


  /* Schema build for MME table*/
    def mmecatalog(table_name: String) = s"""{
    "table":{"namespace":"default", "name":${table_name}},
    "rowkey":"key",
    "columns":{
      "userid_mme_seq":{"cf":"rowkey", "col":"key", "type":"string"},
      "userid_mme":{"cf":"cfLocation", "col":"emsisdn", "type":"string"},
      "lkey_mme":{"cf":"cfLocation", "col":"lkey", "type":"string"},
      "partition_mme":{"cf":"cfLocation", "col":"partitionNum", "type":"string"},
      "time_mme":{"cf":"cfLocation", "col":"starttime", "type":"string"}
    }
  }""".stripMargin

  /* Schema build for MME table*/
  def timeStampCatalog(table_name: String,colfam: String, column: String) = s"""{
    "table":{"namespace":"default", "name":${table_name}},
    "rowkey":"key",
    "columns":{
      "rowkey_time":{"cf":"rowkey", "col":"key", "type":"string"},
      "ts_time":{"cf":${colfam}, "col":${column}, "type":"string"}
    }
  }""".stripMargin

  def webipfr_enrich_control(table_name: String) = s"""{
    "table":{"namespace":"default", "name":${table_name}},
    "rowkey":"key",
    "columns":{
      "control_key":{"cf":"rowkey", "col":"key", "type":"string"},
      "weblogs_stream_processed_ts":{"cf":"cfEnrich", "col":"weblogs_stream_processed_ts", "type":"string"},
      "weblogs_stream_processed_systime":{"cf":"cfEnrich", "col":"weblogs_stream_processed_systime", "type":"string"},
      "weblogs_batch_processed_ts":{"cf":"cfEnrich", "col":"weblogs_batch_processed_ts", "type":"string"},
      "weblogs_batch_processed_systime":{"cf":"cfEnrich", "col":"weblogs_batch_processed_systime", "type":"string"},
      "mme_deleted_systime":{"cf":"cfEnrich", "col":"mme_deleted_systime", "type":"string"},
      "mme_deleted_ts":{"cf":"cfEnrich", "col":"mme_deleted_ts", "type":"string"}
    }
  }""".stripMargin

}