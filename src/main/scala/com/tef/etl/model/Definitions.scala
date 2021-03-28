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

  val deviceDBSchema:StructType = StructType(Array(
    StructField("emsisdn", StringType, nullable = true),
    StructField("imsi", StringType, nullable = true),
    StructField("imei", StringType, nullable = true),
    StructField("marketing_name", StringType, nullable = true),
    StructField("internal_model_name", StringType, nullable = true),
    StructField("manufacturer", StringType, nullable = true),
    StructField("bands", StringType, nullable = true),
    StructField("allocation_date", StringType, nullable = true),
    StructField("brand_name", StringType, nullable = true),
    StructField("model_name", StringType, nullable = true),
    StructField("operating_system", StringType, nullable = true),
    StructField("nfc", StringType, nullable = true),
    StructField("bluetooth", StringType, nullable = true),
    StructField("wlan", StringType, nullable = true),
    StructField("device_type", StringType, nullable = true),
    StructField("service_provider_id", StringType, nullable = true),
    StructField("tariff_offering_id", StringType, nullable = true),
    StructField("billing_system", StringType, nullable = true)
  ))

/*  val nonlkeySchema:StructType = StructType(Array(
    StructField("clientip",StringType, nullable = true),
    StructField("clientport",StringType, nullable = true),
    StructField("serverlocport",StringType, nullable = true),
    StructField("serverlocalegress",StringType, nullable = true),
    StructField("clientvlanid",StringType, nullable = true),
    StructField("serverip",StringType, nullable = true),
    StructField("serverport",StringType, nullable = true),
    StructField("serverincport",StringType, nullable = true),
    StructField("servervlanid",StringType, nullable = true),
    StructField("transactiontime" ,StringType, nullable = true),
    StructField("responsetime"      ,StringType, nullable = true),
    StructField("compressiontime" ,StringType, nullable = true),
    StructField("subdatalkp" ,StringType, nullable = true),
    StructField("dnslkp" ,StringType, nullable = true),
    StructField("redinvocation" ,StringType, nullable = true),
    StructField("uaproftime" ,StringType, nullable = true),
    StructField("exdblkp" ,StringType, nullable = true),
    StructField("cattime" ,StringType, nullable = true),
    StructField("analyticstime" ,StringType, nullable = true),
    StructField("reqmod",StringType, nullable = true),
    StructField("respmod",StringType, nullable = true),
    StructField("fit",StringType, nullable = true),
    StructField("contentservrestime",StringType, nullable = true),
    StructField("optflag1",StringType, nullable = true),
    StructField("optflag2",StringType, nullable = true),
    StructField("optflag3",StringType, nullable = true),
    StructField("optflag4",StringType, nullable = true),
    StructField("optflag5",StringType, nullable = true),
    StructField("optflag6",StringType, nullable = true),
    StructField("optflag7",StringType, nullable = true),
    StructField("optflag8",StringType, nullable = true),
    StructField("optflag9",StringType, nullable = true),
    StructField("optflag10",StringType, nullable = true),
    StructField("optflag11",StringType, nullable = true),
    StructField("optflag12",StringType, nullable = true),
    StructField("optflag13",StringType, nullable = true),
    StructField("optflag14",StringType, nullable = true),
    StructField("optflag15",StringType, nullable = true),
    StructField("optflag16",StringType, nullable = true),
    StructField("optflag17",StringType, nullable = true),
    StructField("optflag18",StringType, nullable = true),
    StructField("optflag19",StringType, nullable = true),
    StructField("optflag20",StringType, nullable = true),
    StructField("optflag21",StringType, nullable = true),
    StructField("optflag22",StringType, nullable = true),
    StructField("optflag23",StringType, nullable = true),
    StructField("selflag1",StringType, nullable = true),
    StructField("selflag2",StringType, nullable = true),
    StructField("selflag3",StringType, nullable = true),
    StructField("selflag4",StringType, nullable = true),
    StructField("selflag5",StringType, nullable = true),
    StructField("selflag6",StringType, nullable = true),
    StructField("selflag7",StringType, nullable = true),
    StructField("selflag8",StringType, nullable = true),
    StructField("selflag9",StringType, nullable = true),
    StructField("selflag10",StringType, nullable = true),
    StructField("selflag11",StringType, nullable = true),
    StructField("selflag12",StringType, nullable = true),
    StructField("selflag13",StringType, nullable = true),
    StructField("selflag14",StringType, nullable = true),
    StructField("selflag15",StringType, nullable = true),
    StructField("selflag16",StringType, nullable = true),
    StructField("selflag17",StringType, nullable = true),
    StructField("selflag18",StringType, nullable = true),
    StructField("selflag19",StringType, nullable = true),
    StructField("selflag20",StringType, nullable = true),
    StructField("selflag21",StringType, nullable = true),
    StructField("selflag22",StringType, nullable = true),
    StructField("selflag23",StringType, nullable = true),
    StructField("selflag24",StringType, nullable = true),
    StructField("gfflag1",StringType, nullable = true),
    StructField("gfflag2",StringType, nullable = true),
    StructField("gfflag3",StringType, nullable = true),
    StructField("gfflag4",StringType, nullable = true),
    StructField("gfflag5",StringType, nullable = true),
    StructField("gfflag6",StringType, nullable = true),
    StructField("gfflag7",StringType, nullable = true),
    StructField("gfflag8",StringType, nullable = true),
    StructField("gfflag9",StringType, nullable = true),
    StructField("gfflag10",StringType, nullable = true),
    StructField("gfflag11",StringType, nullable = true),
    StructField("gfflag12",StringType, nullable = true),
    StructField("gfflag13",StringType, nullable = true),
    StructField("gfflag14",StringType, nullable = true),
    StructField("gfflag15",StringType, nullable = true),
    StructField("gfflag16",StringType, nullable = true),
    StructField("gfflag17",StringType, nullable = true),
    StructField("gfflag18",StringType, nullable = true),
    StructField("gfflag19",StringType, nullable = true),
    StructField("gfflag20",StringType, nullable = true),
    StructField("gfflag21",StringType, nullable = true),
    StructField("gfflag22",StringType, nullable = true),
    StructField("gfflag23",StringType, nullable = true),
    StructField("gfflag24",StringType, nullable = true),
    StructField("gfflag25",StringType, nullable = true),
    StructField("gfflag26",StringType, nullable = true),
    StructField("gfflag27",StringType, nullable = true),
    StructField("gfflag28",StringType, nullable = true),
    StructField("gfflag29",StringType, nullable = true),
    StructField("gfflag30",StringType, nullable = true),
    StructField("gfflag31",StringType, nullable = true),
    StructField("gfflag32",StringType, nullable = true),
    StructField("gfflag33",StringType, nullable = true),
    StructField("gfflag34",StringType, nullable = true),
    StructField("gfflag35",StringType, nullable = true),
    StructField("vslsessid",StringType, nullable = true),
    StructField("vslconntxns",StringType, nullable = true),
    StructField("vslmedtxns",StringType, nullable = true),
    StructField("vslnrdntxns",StringType, nullable = true),
    StructField("vslsessflag",StringType, nullable = true),
    StructField("vslhrzres",StringType, nullable = true),
    StructField("vslvertres",StringType, nullable = true),
    StructField("vslfrmsze",StringType, nullable = true),
    StructField("vslfrmrte",StringType, nullable = true),
    StructField("vslmedtm",StringType, nullable = true),
    StructField("vslsapreqb",StringType, nullable = true),
    StructField("vslsaprspb",StringType, nullable = true),
    StructField("vslpcrte",StringType, nullable = true),
    StructField("vslstarttstmp",StringType, nullable = true),
    StructField("httpmethod",StringType, nullable = true),
    StructField("domain",StringType, nullable = true),
    StructField("url",StringType, nullable = true),
    StructField("query",StringType, nullable = true),
    StructField("urlfcid",StringType, nullable = true),
    StructField("urlfcgid",StringType, nullable = true),
    StructField("urlfcrep",StringType, nullable = true),
    StructField("urlfrspa",StringType, nullable = true),
    StructField("urlfstmtc",StringType, nullable = true),
    StructField("urlfstprv",StringType, nullable = true),
    StructField("matchedurlset",StringType, nullable = true),
    StructField("windowscalingenabled",StringType, nullable = true),
    StructField("sackenabled",StringType, nullable = true),
    StructField("wsfactorsentbyns",StringType, nullable = true),
    StructField("wsfactor",StringType, nullable = true),
    StructField("endpointmode",StringType, nullable = true),
    StructField("rstended",StringType, nullable = true),
    StructField("mss",StringType, nullable = true),
    StructField("nsobspid",StringType, nullable = true),
    StructField("nsobsdid",StringType, nullable = true),
    StructField("nsexppid",StringType, nullable = true),
    StructField("nstrid",StringType, nullable = true),
    StructField("nsvsn",StringType, nullable = true),
    StructField("nsslastupdtstmp",StringType, nullable = true),
    StructField("nsvstcpstarttstmp",StringType, nullable = true),
    StructField("netlabel",StringType, nullable = true),
    StructField("tcpprofile",StringType, nullable = true),
    StructField("conglev",StringType, nullable = true),
    StructField("conglevclass",StringType, nullable = true),
    StructField("signalqual",StringType, nullable = true),
    StructField("signalqualclass",StringType, nullable = true),
    StructField("version",StringType, nullable = true),
    StructField("pid",StringType, nullable = true),
    StructField("rsize",StringType, nullable = true),
    StructField("srsize",StringType, nullable = true),
    StructField("cid",StringType, nullable = true),
    StructField("originalsize",StringType, nullable = true),
    StructField("optimisedsize",StringType, nullable = true),
    StructField("compressionpercent",StringType, nullable = true),
    StructField("cachevalidbytes",StringType, nullable = true),
    StructField("cachestatuscode",StringType, nullable = true),
    StructField("httpcode",StringType, nullable = true),
    StructField("socketunreadsize",StringType, nullable = true),
    StructField("socketunsentsize",StringType, nullable = true),
    StructField("medialogstring",StringType, nullable = true),
    StructField("src_flag",StringType, nullable = true),
    StructField("src_tcpsl",StringType, nullable = true),
    StructField("contenttype",StringType, nullable = true),
    StructField("MSH",StringType, nullable = true),
    StructField("sessionid",StringType, nullable = true),
    StructField("susbcriberid",StringType, nullable = true),
    StructField("useragent",StringType, nullable = true),
    StructField("deviceid",StringType, nullable = true),
    StructField("uagroup",StringType, nullable = true),
    StructField("catid",StringType, nullable = true),
    StructField("httpref",StringType, nullable = true),
    StructField("customrepgrp",StringType, nullable = true),
    StructField("customrepopt1",StringType, nullable = true),
    StructField("customrepopt2",StringType, nullable = true),
    StructField("applicationid",StringType, nullable = true),
    StructField("uxichannelbandwidth",StringType, nullable = true),
    StructField("cellid",StringType, nullable = true),
    StructField("cellcongestionlevel",StringType, nullable = true),
    StructField("optsource",StringType, nullable = true),
    StructField("predicteduagroup",StringType, nullable = true),
    StructField("sslinfolog",StringType, nullable = true),
    StructField("timestamp",StringType, nullable = true)
    ))*/

}
