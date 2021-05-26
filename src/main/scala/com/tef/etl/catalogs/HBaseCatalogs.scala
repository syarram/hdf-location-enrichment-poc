package com.tef.etl.catalogs
/* @author Mohamed Bilal

 *
 */
object HBaseCatalogs {

  /* Schema build for Web table*/
  def stagewebcatalog(table_name: String) =
    s"""{
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
      "sessionid":{"cf":"cfNonLKey", "col":"sessionid", "type":"string"},
      "clientip":{"cf":"cfNonLKey", "col":"clientip", "type":"string"}
    }
  }""".stripMargin

  def stageRadiusCatalog(table_name: String) =
    s"""{
    "table":{"namespace":"default", "name":${table_name}},
    "rowkey":"key",
    "columns":{
      "rkey":{"cf":"rowkey", "col":"key", "type":"string"},
      "ts":{"cf":"cfRadius", "col":"TS", "type":"string"},
      "cc":{"cf":"cfRadius", "col":"CC", "type":"string"},
      "avpcustom1":{"cf":"cfRadius", "col":"AVP1", "type":"string"},
      "avpcustom2":{"cf":"cfRadius", "col":"AVP2", "type":"string"},
      "avpcustom3":{"cf":"cfRadius", "col":"AVP3", "type":"string"},
      "avpcustom4":{"cf":"cfRadius", "col":"AVP4", "type":"string"},
      "avpcustom5":{"cf":"cfRadius", "col":"AVP5", "type":"string"}
    }
  }""".stripMargin

  def cspCatalog(table_name: String): String =
    s"""{
    "table":{"namespace":"default", "name":${table_name}},
    "rowkey":"key",
    "columns":{
      "ip":{"cf":"rowkey", "col":"key", "type":"string"},
      "apn_name":{"cf":"cfcspapn", "col":"apn", "type":"string"},
      "apnid":{"cf":"cfcspapn", "col":"apn-name", "type":"string"},
      "csp":{"cf":"cfcspapn", "col":"csp", "type":"str  ing"},
      "network":{"cf":"cfcspapn", "col":"network", "type":"string"}
    }
  }""".stripMargin


  def stageTrnsactionKeys(table_name: String) =
    s"""{
    "table":{"namespace":"default", "name":${table_name}},
    "rowkey":"key",
    "columns":{
      "userid_web_seq":{"cf":"rowkey", "col":"key", "type":"string"}
    }
  }""".stripMargin


  /* Schema build for MME table*/
  def mmecatalog(table_name: String) =
    s"""{
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
  def timeStampCatalog(table_name: String, colfam: String, column: String) =
    s"""{
    "table":{"namespace":"default", "name":${table_name}},
    "rowkey":"key",
    "columns":{
      "rowkey_time":{"cf":"rowkey", "col":"key", "type":"string"},
      "ts_time":{"cf":${colfam}, "col":${column}, "type":"string"}
    }
  }""".stripMargin

  def controlCatalog(table_name: String) =
    s"""{
    "table":{"namespace":"default", "name":${table_name}},
    "rowkey":"key",
    "columns":{
      "control_key":{"cf":"rowkey", "col":"key", "type":"string"},
      "weblogs_stream_processed_ts":{"cf":"cfEnrich", "col":"weblogs_stream_processed_ts", "type":"string"},
      "weblogs_stream_processed_systime":{"cf":"cfEnrich", "col":"weblogs_stream_processed_systime", "type":"string"},
      "weblogs_batch_processed_ts":{"cf":"cfEnrich", "col":"weblogs_batch_processed_ts", "type":"string"},
      "weblogs_batch_processed_systime":{"cf":"cfEnrich", "col":"weblogs_batch_processed_systime", "type":"string"},
      "mme_deleted_systime":{"cf":"cfEnrich", "col":"mme_deleted_systime", "type":"string"},
      "mme_deleted_ts":{"cf":"cfEnrich", "col":"mme_deleted_ts", "type":"string"},
      "batch_job_status":{"cf":"cfEnrich", "col":"batch_job_status", "type":"string"},
      "mme_delete_job_status":{"cf":"cfEnrich", "col":"mme_delete_job_status", "type":"string"},
      "radius_delete_job_status":{"cf":"cfEnrich", "col":"radius_delete_job_status", "type":"string"}
    }
  }""".stripMargin

  def houseKeepingCatalog(table_name: String, processName: String):String = {
    processName match{
     case "MME" => s"""{
        "table":{"namespace":"default", "name":${table_name}},
        "rowkey":"key",
        "columns":{
          "control_key":{"cf":"rowkey", "col":"key", "type":"string"},
          "mme_deleted_ts":{"cf":"cfEnrich", "col":"mme_deleted_ts", "type":"string"},
          "mme_delete_job_status":{"cf":"cfEnrich", "col":"mme_delete_job_status", "type":"string"}
        }
      }""".stripMargin
     case "Radius" => s"""{
        "table":{"namespace":"default", "name":${table_name}},
        "rowkey":"key",
        "columns":{
          "control_key":{"cf":"rowkey", "col":"key", "type":"string"},
          "radius_deleted_ts":{"cf":"cfEnrich", "col":"radius_deleted_ts", "type":"string"},
          "radius_delete_job_status":{"cf":"cfEnrich", "col":"radius_delete_job_status", "type":"string"}
        }
      }""".stripMargin
     case "Web" => s"""{
        "table":{"namespace":"default", "name":${table_name}},
        "rowkey":"key",
        "columns":{
          "control_key":{"cf":"rowkey", "col":"key", "type":"string"},
          "weblogs_batch_processed_ts":{"cf":"cfEnrich", "col":"weblogs_batch_processed_ts", "type":"string"},
          "batch_job_status":{"cf":"cfEnrich", "col":"batch_job_status", "type":"string"}
        }
      }""".stripMargin
    }
  }

}