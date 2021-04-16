package com.tef.etl.weblogs

import com.tef.etl.catalogs.TargetCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object TransactionDFOperations {

  def sourceColumnSplit(spark:SparkSession, df: DataFrame, fileType:String="MME"): DataFrame = {
    val df1 = //df.withColumn("clientip",split(col("nonlkey_cols"),"\\|").getItem(0))
      df.withColumn("clientport",split(col("nonlkey_cols"),"\\|").getItem(1))
      .withColumn("serverlocport",split(col("nonlkey_cols"),"\\|").getItem(2))
      .withColumn("serverlocalegress",split(col("nonlkey_cols"),"\\|").getItem(3))
      .withColumn("clientvlanid",split(col("nonlkey_cols"),"\\|").getItem(4))
      .withColumn("serverip",split(col("nonlkey_cols"),"\\|").getItem(5))
      .withColumn("serverport",split(col("nonlkey_cols"),"\\|").getItem(6))
      .withColumn("serverincport",split(col("nonlkey_cols"),"\\|").getItem(7))
      .withColumn("servervlanid",split(col("nonlkey_cols"),"\\|").getItem(8))
      .withColumn("transactiontime",split(col("nonlkey_cols"),"\\|").getItem(9))
      .withColumn("responsetime",split(col("nonlkey_cols"),"\\|").getItem(10))
      .withColumn("compressiontime",split(col("nonlkey_cols"),"\\|").getItem(11))
      .withColumn("subdatalkp",split(col("nonlkey_cols"),"\\|").getItem(12))
      .withColumn("dnslkp",split(col("nonlkey_cols"),"\\|").getItem(13))
      .withColumn("redinvocation",split(col("nonlkey_cols"),"\\|").getItem(14))
      .withColumn("uaproftime",split(col("nonlkey_cols"),"\\|").getItem(15))
      .withColumn("exdblkp",split(col("nonlkey_cols"),"\\|").getItem(16))
      .withColumn("cattime",split(col("nonlkey_cols"),"\\|").getItem(17))
      .withColumn("analyticstime",split(col("nonlkey_cols"),"\\|").getItem(18))
      .withColumn("reqmod",split(col("nonlkey_cols"),"\\|").getItem(19))
      .withColumn("respmod",split(col("nonlkey_cols"),"\\|").getItem(20))
      .withColumn("fit",split(col("nonlkey_cols"),"\\|").getItem(21))
      .withColumn("contentservrestime",split(col("nonlkey_cols"),"\\|").getItem(22))
      .withColumn("optflag1",split(col("nonlkey_cols"),"\\|").getItem(23))
      .withColumn("optflag2",split(col("nonlkey_cols"),"\\|").getItem(24))
      .withColumn("optflag3",split(col("nonlkey_cols"),"\\|").getItem(25))
      .withColumn("optflag4",split(col("nonlkey_cols"),"\\|").getItem(26))
      .withColumn("optflag5",split(col("nonlkey_cols"),"\\|").getItem(27))
      .withColumn("optflag6",split(col("nonlkey_cols"),"\\|").getItem(28))
      .withColumn("optflag7",split(col("nonlkey_cols"),"\\|").getItem(29))
      .withColumn("optflag8",split(col("nonlkey_cols"),"\\|").getItem(30))
      .withColumn("optflag9",split(col("nonlkey_cols"),"\\|").getItem(31))
      .withColumn("optflag10",split(col("nonlkey_cols"),"\\|").getItem(32))
      .withColumn("optflag11",split(col("nonlkey_cols"),"\\|").getItem(33))
      .withColumn("optflag12",split(col("nonlkey_cols"),"\\|").getItem(34))
      .withColumn("optflag13",split(col("nonlkey_cols"),"\\|").getItem(35))
      .withColumn("optflag14",split(col("nonlkey_cols"),"\\|").getItem(36))
      .withColumn("optflag15",split(col("nonlkey_cols"),"\\|").getItem(37))
      .withColumn("optflag16",split(col("nonlkey_cols"),"\\|").getItem(38))
      .withColumn("optflag17",split(col("nonlkey_cols"),"\\|").getItem(39))
      .withColumn("optflag18",split(col("nonlkey_cols"),"\\|").getItem(40))
      .withColumn("optflag19",split(col("nonlkey_cols"),"\\|").getItem(41))
      .withColumn("optflag20",split(col("nonlkey_cols"),"\\|").getItem(42))
      .withColumn("optflag21",split(col("nonlkey_cols"),"\\|").getItem(43))
      .withColumn("optflag22",split(col("nonlkey_cols"),"\\|").getItem(44))
      .withColumn("optflag23",split(col("nonlkey_cols"),"\\|").getItem(45))
      .withColumn("selflag1",split(col("nonlkey_cols"),"\\|").getItem(46))
      .withColumn("selflag2",split(col("nonlkey_cols"),"\\|").getItem(47))
      .withColumn("selflag3",split(col("nonlkey_cols"),"\\|").getItem(48))
      .withColumn("selflag4",split(col("nonlkey_cols"),"\\|").getItem(49))
      .withColumn("selflag5",split(col("nonlkey_cols"),"\\|").getItem(50))
   .withColumn("selflag6",split(col("nonlkey_cols"),"\\|").getItem(51))
   .withColumn("selflag7",split(col("nonlkey_cols"),"\\|").getItem(52))
   .withColumn("selflag8",split(col("nonlkey_cols"),"\\|").getItem(53))
   .withColumn("selflag9",split(col("nonlkey_cols"),"\\|").getItem(54))
   .withColumn("selflag10",split(col("nonlkey_cols"),"\\|").getItem(55))
   .withColumn("selflag11",split(col("nonlkey_cols"),"\\|").getItem(56))
   .withColumn("selflag12",split(col("nonlkey_cols"),"\\|").getItem(57))
   .withColumn("selflag13",split(col("nonlkey_cols"),"\\|").getItem(58))
   .withColumn("selflag14",split(col("nonlkey_cols"),"\\|").getItem(59))
   .withColumn("selflag15",split(col("nonlkey_cols"),"\\|").getItem(60))
   .withColumn("selflag16",split(col("nonlkey_cols"),"\\|").getItem(61))
   .withColumn("selflag17",split(col("nonlkey_cols"),"\\|").getItem(62))
   .withColumn("selflag18",split(col("nonlkey_cols"),"\\|").getItem(63))
   .withColumn("selflag19",split(col("nonlkey_cols"),"\\|").getItem(64))
   .withColumn("selflag20",split(col("nonlkey_cols"),"\\|").getItem(65))
   .withColumn("selflag21",split(col("nonlkey_cols"),"\\|").getItem(66))
   .withColumn("selflag22",split(col("nonlkey_cols"),"\\|").getItem(67))
   .withColumn("selflag23",split(col("nonlkey_cols"),"\\|").getItem(68))
   .withColumn("selflag24",split(col("nonlkey_cols"),"\\|").getItem(69))
   .withColumn("gfflag1",split(col("nonlkey_cols"),"\\|").getItem(70))
   .withColumn("gfflag2",split(col("nonlkey_cols"),"\\|").getItem(71))
   .withColumn("gfflag3",split(col("nonlkey_cols"),"\\|").getItem(72))
   .withColumn("gfflag4",split(col("nonlkey_cols"),"\\|").getItem(73))
   .withColumn("gfflag5",split(col("nonlkey_cols"),"\\|").getItem(74))
   .withColumn("gfflag6",split(col("nonlkey_cols"),"\\|").getItem(75))
   .withColumn("gfflag7",split(col("nonlkey_cols"),"\\|").getItem(76))
   .withColumn("gfflag8",split(col("nonlkey_cols"),"\\|").getItem(77))
   .withColumn("gfflag9",split(col("nonlkey_cols"),"\\|").getItem(78))
   .withColumn("gfflag10",split(col("nonlkey_cols"),"\\|").getItem(79))
   .withColumn("gfflag11",split(col("nonlkey_cols"),"\\|").getItem(80))
   .withColumn("gfflag12",split(col("nonlkey_cols"),"\\|").getItem(81))
   .withColumn("gfflag13",split(col("nonlkey_cols"),"\\|").getItem(82))
   .withColumn("gfflag14",split(col("nonlkey_cols"),"\\|").getItem(83))
   .withColumn("gfflag15",split(col("nonlkey_cols"),"\\|").getItem(84))
   .withColumn("gfflag16",split(col("nonlkey_cols"),"\\|").getItem(85))
   .withColumn("gfflag17",split(col("nonlkey_cols"),"\\|").getItem(86))
   .withColumn("gfflag18",split(col("nonlkey_cols"),"\\|").getItem(87))
   .withColumn("gfflag19",split(col("nonlkey_cols"),"\\|").getItem(88))
   .withColumn("gfflag20",split(col("nonlkey_cols"),"\\|").getItem(89))
   .withColumn("gfflag21",split(col("nonlkey_cols"),"\\|").getItem(90))
   .withColumn("gfflag22",split(col("nonlkey_cols"),"\\|").getItem(91))
   .withColumn("gfflag23",split(col("nonlkey_cols"),"\\|").getItem(92))
   .withColumn("gfflag24",split(col("nonlkey_cols"),"\\|").getItem(93))
   .withColumn("gfflag25",split(col("nonlkey_cols"),"\\|").getItem(94))
   .withColumn("gfflag26",split(col("nonlkey_cols"),"\\|").getItem(164))
   .withColumn("gfflag27",split(col("nonlkey_cols"),"\\|").getItem(96))
   .withColumn("gfflag28",split(col("nonlkey_cols"),"\\|").getItem(97))
   .withColumn("gfflag29",split(col("nonlkey_cols"),"\\|").getItem(98))
   .withColumn("gfflag30",split(col("nonlkey_cols"),"\\|").getItem(99))
   .withColumn("gfflag31",split(col("nonlkey_cols"),"\\|").getItem(100))
   .withColumn("gfflag32",split(col("nonlkey_cols"),"\\|").getItem(101))
  .withColumn("gfflag33",split(col("nonlkey_cols"),"\\|").getItem(102))
   .withColumn("gfflag34",split(col("nonlkey_cols"),"\\|").getItem(103))
   .withColumn("gfflag35",split(col("nonlkey_cols"),"\\|").getItem(104))
   .withColumn("vslsessid",split(col("nonlkey_cols"),"\\|").getItem(105))
   .withColumn("vslconntxns",split(col("nonlkey_cols"),"\\|").getItem(106))
   .withColumn("vslmedtxns",split(col("nonlkey_cols"),"\\|").getItem(107))
   .withColumn("vslnrdntxns",split(col("nonlkey_cols"),"\\|").getItem(108))
   .withColumn("vslsessflag",split(col("nonlkey_cols"),"\\|").getItem(109))
   .withColumn("vslhrzres",split(col("nonlkey_cols"),"\\|").getItem(110))
   .withColumn("vslvertres",split(col("nonlkey_cols"),"\\|").getItem(111))
   .withColumn("vslfrmsze",split(col("nonlkey_cols"),"\\|").getItem(112))
   .withColumn("vslfrmrte",split(col("nonlkey_cols"),"\\|").getItem(113))
   .withColumn("vslmedtm",split(col("nonlkey_cols"),"\\|").getItem(114))
   .withColumn("vslsapreqb",split(col("nonlkey_cols"),"\\|").getItem(115))
   .withColumn("vslsaprspb",split(col("nonlkey_cols"),"\\|").getItem(116))
   .withColumn("vslpcrte",split(col("nonlkey_cols"),"\\|").getItem(117))
   .withColumn("vslstarttstmp",split(col("nonlkey_cols"),"\\|").getItem(118))
   .withColumn("httpmethod",split(col("nonlkey_cols"),"\\|").getItem(119))
   .withColumn("domain",split(col("nonlkey_cols"),"\\|").getItem(120))
   .withColumn("url",split(col("nonlkey_cols"),"\\|").getItem(121))
   .withColumn("query",split(col("nonlkey_cols"),"\\|").getItem(122))
   .withColumn("urlfcid",split(col("nonlkey_cols"),"\\|").getItem(123))
   .withColumn("urlfcgid",split(col("nonlkey_cols"),"\\|").getItem(124))
   .withColumn("urlfcrep",split(col("nonlkey_cols"),"\\|").getItem(125))
   .withColumn("urlfrspa",split(col("nonlkey_cols"),"\\|").getItem(126))
   .withColumn("urlfstmtc",split(col("nonlkey_cols"),"\\|").getItem(127))
   .withColumn("urlfstprv",split(col("nonlkey_cols"),"\\|").getItem(128))
   .withColumn("matchedurlset",split(col("nonlkey_cols"),"\\|").getItem(129))
   .withColumn("windowscalingenabled",split(col("nonlkey_cols"),"\\|").getItem(130))
   .withColumn("sackenabled",split(col("nonlkey_cols"),"\\|").getItem(131))
   .withColumn("wsfactorsentbyns",split(col("nonlkey_cols"),"\\|").getItem(132))
   .withColumn("wsfactor",split(col("nonlkey_cols"),"\\|").getItem(133))
   .withColumn("endpointmode",split(col("nonlkey_cols"),"\\|").getItem(134))
   .withColumn("rstended",split(col("nonlkey_cols"),"\\|").getItem(135))
   .withColumn("mss",split(col("nonlkey_cols"),"\\|").getItem(136))
   .withColumn("nsobspid",split(col("nonlkey_cols"),"\\|").getItem(137))
   .withColumn("nsobsdid",split(col("nonlkey_cols"),"\\|").getItem(138))
   .withColumn("nsexppid",split(col("nonlkey_cols"),"\\|").getItem(139))
   .withColumn("nstrid",split(col("nonlkey_cols"),"\\|").getItem(140))
   .withColumn("nsvsn",split(col("nonlkey_cols"),"\\|").getItem(141))
   .withColumn("nsslastupdtstmp",split(col("nonlkey_cols"),"\\|").getItem(142))
   .withColumn("nsvstcpstarttstmp",split(col("nonlkey_cols"),"\\|").getItem(143))
   .withColumn("tcpprofile",split(col("nonlkey_cols"),"\\|").getItem(144))
   .withColumn("netlabel",split(col("nonlkey_cols"),"\\|").getItem(145))
   .withColumn("conglev",split(col("nonlkey_cols"),"\\|").getItem(146))
   .withColumn("conglevclass",split(col("nonlkey_cols"),"\\|").getItem(147))
   .withColumn("signalqual",split(col("nonlkey_cols"),"\\|").getItem(148))
   .withColumn("signalqualclass",split(col("nonlkey_cols"),"\\|").getItem(149))
   .withColumn("version",split(col("nonlkey_cols"),"\\|").getItem(150))
   .withColumn("pid",split(col("nonlkey_cols"),"\\|").getItem(151))
   .withColumn("rsize",split(col("nonlkey_cols"),"\\|").getItem(152))
   .withColumn("srsize",split(col("nonlkey_cols"),"\\|").getItem(153))
   .withColumn("cid",split(col("nonlkey_cols"),"\\|").getItem(154))
   .withColumn("originalsize",split(col("nonlkey_cols"),"\\|").getItem(155))
   .withColumn("optimisedsize",split(col("nonlkey_cols"),"\\|").getItem(156))
   .withColumn("compressionpercent",split(col("nonlkey_cols"),"\\|").getItem(157))
   .withColumn("cachevalidbytes",split(col("nonlkey_cols"),"\\|").getItem(158))
   .withColumn("cachestatuscode",split(col("nonlkey_cols"),"\\|").getItem(159))
   .withColumn("httpcode",split(col("nonlkey_cols"),"\\|").getItem(160))
   .withColumn("socketunreadsize",split(col("nonlkey_cols"),"\\|").getItem(161))
   .withColumn("socketunsentsize",split(col("nonlkey_cols"),"\\|").getItem(162))
   .withColumn("medialogstring",split(col("nonlkey_cols"),"\\|").getItem(163))
   .withColumn("src_flag",split(col("nonlkey_cols"),"\\|").getItem(164))
   .withColumn("src_tcpsl",split(col("nonlkey_cols"),"\\|").getItem(165))
   .withColumn("contenttype",split(col("nonlkey_cols"),"\\|").getItem(166))
   .withColumn("MSH",split(col("nonlkey_cols"),"\\|").getItem(167))
   //.withColumn("sessionid",split(col("nonlkey_cols"),"\\|").getItem(168))
   .withColumn("susbcriberid",split(col("nonlkey_cols"),"\\|").getItem(169))
   .withColumn("useragent",split(col("nonlkey_cols"),"\\|").getItem(170))
   .withColumn("deviceid",split(col("nonlkey_cols"),"\\|").getItem(171))
   .withColumn("uagroup",split(col("nonlkey_cols"),"\\|").getItem(172))
   .withColumn("catid",split(col("nonlkey_cols"),"\\|").getItem(173))
   .withColumn("httpref",split(col("nonlkey_cols"),"\\|").getItem(174))
   .withColumn("customrepgrp",split(col("nonlkey_cols"),"\\|").getItem(175))
   .withColumn("customrepopt1",split(col("nonlkey_cols"),"\\|").getItem(176))
   .withColumn("customrepopt2",split(col("nonlkey_cols"),"\\|").getItem(177))
   .withColumn("applicationid",split(col("nonlkey_cols"),"\\|").getItem(178))
   .withColumn("uxichannelbandwidth",split(col("nonlkey_cols"),"\\|").getItem(179))
   .withColumn("cellid",
     when(col("lkey").equalTo("1090-79999"),"")
       .otherwise("N")
   )
   .withColumn("cellcongestionlevel",split(col("nonlkey_cols"),"\\|").getItem(181))
   .withColumn("optsource",split(col("nonlkey_cols"),"\\|").getItem(182))
   .withColumn("predicteduagroup",split(col("nonlkey_cols"),"\\|").getItem(183))
   .withColumn("sslinfolog",split(col("nonlkey_cols"),"\\|").getItem(184))
   .withColumn("timestamp_src",split(col("nonlkey_cols"),"\\|").getItem(185))
   .withColumn("foundurl",lit("Null")).drop("nonlkey_cols")

    enrichTCPSL(df1).withColumn("sizetag", when(col("optimisedsize") < 1000,"Tiny")
      .when(col("optimisedsize") >= 1000 && col("optimisedsize") < 200000,"Small")
    .when(col("optimisedsize") >= 200000 && col("optimisedsize") < 1000000,"Medium")
    .when(col("optimisedsize") >= 1000000 && col("optimisedsize") < 5000000,"Large")
    .when(col("optimisedsize") >= 5000000, "Huge").otherwise("-"))
      .withColumn("flag",
        when(col("src_flag").equalTo(0),"NS_Uncategorized")
        .when(col("src_flag").equalTo(29),"NS_Encoded_ABR_video")
        .when(col("src_flag").equalTo(30),"NS_Clear-text_PD_video")
        .when(col("src_flag").equalTo(31),"NS_Clear-text_ABR_video")
        .when(col("src_flag").equalTo(32),"NS_Other_video")
        .when(col("src_flag").equalTo(33),"NS_QUIC_ABR_video")
          .otherwise("Not_Detected"))
      .withColumn("conttype_1", when(
        split(col("contenttype"),"/")(0).equalTo("-"),"unknown")
        .otherwise(split(col("contenttype"),"/")(0)))
      .withColumn("conttype", lit(split(col("contenttype"),"/")(1).toString().split(";")(0)))
      .withColumn("dmy", to_date(col("timestamp_src")))
      .withColumn("hh", date_format(col("timestamp_src"),"HH"))
      .withColumn("mm", minute(col("timestamp_src")))
      .withColumn("ss", second(col("timestamp_src")))
      .withColumn("ms", split(col("timestamp_src"),"\\.")(1).divide(1000))
      .withColumn("appthroughput",
        when(
          col("transactiontime") > 5000 && col("optimisedsize") > 3000000,
          (col("optimisedsize")*8).divide(col("transactiontime"))
        ).otherwise(0))
      .withColumn("tcpthroughput",
        when(
          col("avgrtt") > 10 ,
        (col("avgbif")*8000).divide(col("avgrtt")*(col("pktretransrate")+1))
      ).otherwise(""))
  }

  def enrichTCPSL(df:DataFrame): DataFrame ={
    df.withColumn("minrtt",
      when (split(col("src_tcpsl"),"/")(0).equalTo("-"), null).
        otherwise(split(col("src_tcpsl"),"/")(0)))
      .withColumn("avgrtt", split(col("src_tcpsl"),"/")(1))
      .withColumn("tmp_1", split(col("src_tcpsl"),"/")(2))
      .withColumn("tmp_2", split(col("src_tcpsl"),"/")(3))
      .withColumn("maxrtt", split(col("tmp_1")," ")(0))
      .withColumn("bdp", split(col("tmp_1")," ")(1))
      .withColumn("avgbif", split(col("tmp_1")," ")(2))
      .withColumn("maxbif", split(col("tmp_2")," ")(0))
      .withColumn("pktlossrate", split(col("tmp_2")," ")(1))
      .withColumn("pktretransrate", split(col("src_tcpsl"),"/")(4))
      .drop("tmp_1").drop("tmp_2").drop("src_tcpsl")
  }

  /*
  def enrichMME(df:DataFrame, lookupDF:DataFrame):DataFrame={
    df.join(lookupDF,df("userid_web")===lookupDF("userid_mme"),"left").drop(lookupDF("userid_mme"))
  }

  def enrichAPNID(df:DataFrame, lookupDF:DataFrame):DataFrame={
    df.join(lookupDF,df("clientip")===lookupDF("ip"),"left").
      drop("ip","apn-name","network")
  }

  def enrichMagnet(df:DataFrame, lookupDF:DataFrame):DataFrame={
    val ldf = lookupDF.select("lkey",
      "csr","cell_id","sector","generation","manufacturer","lacod","postcode",
      "easting", "northing","sac", "rac","ant_height", "ground_height","tilt","elec_tilt",
      "azimuth","enodeb_id","tac","ura")
    df.join(ldf,df("lkey")===ldf("lkey"),"left").drop(ldf("lkey"))
  }

  def enrichDiviceDB(df:DataFrame, lookupDF:DataFrame):DataFrame={
    val ldf = lookupDF.select("emsisdn","imsi","imeisv","marketing_name","brand_name","model_name",
      "operating_system","device_type","offering")
    df.join(ldf,df("userid_web")===ldf("emsisdn"),"left")
  }

  def enrichRadius(df:DataFrame, lookupDF:DataFrame):DataFrame={
    val sDF = lookupDF.withColumn("sesID",concat(split(col("rkey"),":")(0),lit(":")))
    val windowSpec  = Window.partitionBy("sesID").orderBy("ts")
    val rSDF = sDF.withColumn("rank",rank().over(windowSpec)).filter(col("rank")===1)
    df.join(rSDF,df("sessionid")===rSDF("sesID"),"left").drop("rank","rkey","sesID")
  }*/

  def getFinalDF(df:DataFrame):DataFrame={
    val missingColumnsDF = df.withColumn("",lit("Null"))
      .withColumn("calc_1",lit("Null"))
      .withColumn("calc_2",lit("Null"))
      .withColumn("calc_3",lit("Null"))
      .withColumn("calc_4",lit("Null"))
      .withColumn("calc_5",lit("Null"))
      .withColumn("vslsessinb",lit("Null"))
      .withColumn("vslsessoutb",lit("Null"))
      .withColumn("vslstalldur",lit("Null"))
      .withColumn("vslstalltme",lit("Null"))
      .withColumn("vslqtyup",lit("Null"))
      .withColumn("vslqtydwn",lit("Null"))
      .withColumn("vslstltncy",lit("Null"))
      .withColumn("dt", date_format(col("dmy"),"yyyyMMdd"))
      .withColumn("hour", col("hh"))
      .withColumn("timestamp", unix_timestamp(col("timestamp_src"),"yyyy-MM-dd HH:mm:ss"))
    //2020-09-14 09:45:00.310738

    val tgtExpr = TargetCatalog.TargetExpr
    missingColumnsDF.select(tgtExpr.head, tgtExpr.tail:_*)

  }


  def joinForLookUps(trasactionDF: DataFrame, magnetDF: DataFrame, deviceDBDF: DataFrame, cspDF: DataFrame, radiusSRCDF: DataFrame): DataFrame = {

    val magnetSpecificColsDF = magnetDF.select("lkey",
      "csr","cell_id","sector","generation","manufacturer","lacod","postcode",
      "easting", "northing","sac", "rac","ant_height", "ground_height","tilt","elec_tilt",
      "azimuth","enodeb_id","tac","ura")
    val deviceDBSelectColsDF = deviceDBDF.select("emsisdn","imsi","imeisv","marketing_name","brand_name","model_name",
      "operating_system","device_type","offering")
    val radiusSessionIDDF = radiusSRCDF.withColumn("sesID",concat(split(col("rkey"),":")(0),lit(":")))
    val windowSpec  = Window.partitionBy("sesID").orderBy("ts")
    val radiusSessionIDOrderedDF = radiusSessionIDDF.withColumn("rank",rank().over(windowSpec)).filter(col("rank")===1)
    trasactionDF
      .join(cspDF,trasactionDF("clientip")===cspDF("ip"),"left").drop(cspDF("ip"))
      .join(radiusSessionIDOrderedDF,trasactionDF("sessionid")===radiusSessionIDOrderedDF("sesID"),"left").drop("rank","rkey","sesID")
      .join(magnetSpecificColsDF,trasactionDF("lkey")===magnetSpecificColsDF("lkey"),"left").drop(magnetSpecificColsDF("lkey"))
      .join(deviceDBSelectColsDF,trasactionDF("userid_web")===deviceDBSelectColsDF("emsisdn"),"left").drop(deviceDBSelectColsDF("emsisdn"))
      .withColumnRenamed("userid_web","emsisdn")
  }

  def joinWithMME(sourceDFWithoutLkey: DataFrame,locationDF: DataFrame, hdfsPartitions: Int): DataFrame = {
    sourceDFWithoutLkey
      .join(locationDF,
        sourceDFWithoutLkey("userid_web") === locationDF("userid_mme") &&
          locationDF("time_mme") <= sourceDFWithoutLkey("time_web") &&
          sourceDFWithoutLkey("partition_web") === locationDF("partition_mme"),"left_outer")
      .withColumn("lkey", when(locationDF("lkey_mme").isNull,"1090-79999").otherwise(locationDF("lkey_mme")))
      .repartition(hdfsPartitions, col("userid_web"))
      .sortWithinPartitions(col("userid_web_seq"),desc("time_mme"))
      .dropDuplicates(Array("time_web","userid_web_seq"))
    //returning location in column lkey
  }

}
