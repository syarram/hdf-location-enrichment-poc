package com.tef.etl.weblogs

import com.tef.etl.definitions.WeblogDef
import org.apache.spark.sql.functions.{col, split, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TransactionDFOperations {

  def sourceColumnSplit(spark:SparkSession, df: DataFrame, fileType:String="MME"): DataFrame = {
    val colNames = WeblogDef.webColumnNames
    import spark.implicits._
    val df1 = df.withColumn("descArray", split($"nonlkey_cols","\\|"))
    val df2 = df1.select(col("descArray") +: (0 until 185)
      .map(x=>
        x match {
          case 0 => col("descArray")(x).alias("clientport")
          case 1 => col("descArray")(x).alias("clientip")
          case 2 => col("descArray")(x).alias("serverlocport")
          case 3 => col("descArray")(x).alias("serverlocalegress")
          case 4 => col("descArray")(x).alias("clientvlanid")
          case 5 => col("descArray")(x).alias("serverip")
          case 6 => col("descArray")(x).alias("serverport")
          case 7 => col("descArray")(x).alias("serverincport")
          case 8 => col("descArray")(x).alias("servervlanid")
          case 9 => col("descArray")(x).alias("transactiontime")

          case 10 => col("descArray")(x).alias("responsetime")
          case 11 => col("descArray")(x).alias("compressiontime")
          case 12 => col("descArray")(x).alias("subdatalkp")
          case 13 => col("descArray")(x).alias("dnslkp")
          case 14 => col("descArray")(x).alias("redinvocation")
          case 15 => col("descArray")(x).alias("uaproftime")
          case 16 => col("descArray")(x).alias("exdblkp")
          case 17 => col("descArray")(x).alias("cattime")
          case 18 => col("descArray")(x).alias("analyticstime")
          case 19 => col("descArray")(x).alias("reqmod")

          case 20 => col("descArray")(x).alias("respmod")
          case 21 => col("descArray")(x).alias("fit")
          case 22 => col("descArray")(x).alias("contentservrestime")
          case 23 => col("descArray")(x).alias("optflag1")
          case 24 => col("descArray")(x).alias("optflag2")
          case 25 => col("descArray")(x).alias("optflag3")
          case 26 => col("descArray")(x).alias("optflag4")
          case 27 => col("descArray")(x).alias("optflag5")
          case 28 => col("descArray")(x).alias("optflag6")
          case 29 => col("descArray")(x).alias("optflag7")

          case 30 => col("descArray")(x).alias("optflag8")
          case 31 => col("descArray")(x).alias("optflag9")
          case 32 => col("descArray")(x).alias("optflag10")
          case 33 => col("descArray")(x).alias("optflag11")
          case 34 => col("descArray")(x).alias("optflag12")
          case 35 => col("descArray")(x).alias("optflag13")
          case 36 => col("descArray")(x).alias("optflag14")
          case 37 => col("descArray")(x).alias("optflag15")
          case 38 => col("descArray")(x).alias("optflag16")
          case 39 => col("descArray")(x).alias("optflag17")

          case 40 => col("descArray")(x).alias("optflag18")
          case 41 => col("descArray")(x).alias("optflag19")
          case 42 => col("descArray")(x).alias("optflag20")
          case 43 => col("descArray")(x).alias("optflag21")
          case 44 => col("descArray")(x).alias("optflag22")
          case 45 => col("descArray")(x).alias("optflag23")
          case 46 => col("descArray")(x).alias("selflag1")
          case 47 => col("descArray")(x).alias("selflag2")
          case 48 => col("descArray")(x).alias("selflag3")
          case 49 => col("descArray")(x).alias("selflag4")

          case 50 => col("descArray")(x).alias("selflag5")
          case 51 => col("descArray")(x).alias("selflag6")
          case 52 => col("descArray")(x).alias("selflag7")
          case 53 => col("descArray")(x).alias("selflag8")
          case 54 => col("descArray")(x).alias("selflag9")
          case 55 => col("descArray")(x).alias("selflag10")
          case 56 => col("descArray")(x).alias("selflag11")
          case 57 => col("descArray")(x).alias("selflag12")
          case 58 => col("descArray")(x).alias("selflag13")
          case 59 => col("descArray")(x).alias("selflag14")

          case 60 => col("descArray")(x).alias("selflag15")
          case 61 => col("descArray")(x).alias("selflag16")
          case 62 => col("descArray")(x).alias("selflag17")
          case 63 => col("descArray")(x).alias("selflag18")
          case 64 => col("descArray")(x).alias("selflag19")
          case 65 => col("descArray")(x).alias("selflag20")
          case 66 => col("descArray")(x).alias("selflag21")
          case 67 => col("descArray")(x).alias("selflag22")
          case 68 => col("descArray")(x).alias("selflag23")
          case 69 => col("descArray")(x).alias("selflag24")

          case 70 => col("descArray")(x).alias("gfflag1")
          case 71 => col("descArray")(x).alias("gfflag2")
          case 72 => col("descArray")(x).alias("gfflag3")
          case 73 => col("descArray")(x).alias("gfflag4")
          case 74 => col("descArray")(x).alias("gfflag5")
          case 75 => col("descArray")(x).alias("gfflag6")
          case 76 => col("descArray")(x).alias("gfflag7")
          case 77 => col("descArray")(x).alias("gfflag8")
          case 78 => col("descArray")(x).alias("gfflag9")
          case 79 => col("descArray")(x).alias("gfflag10")

          case 80 => col("descArray")(x).alias("gfflag11")
          case 81 => col("descArray")(x).alias("gfflag12")
          case 82 => col("descArray")(x).alias("gfflag13")
          case 83 => col("descArray")(x).alias("gfflag14")
          case 84 => col("descArray")(x).alias("gfflag15")
          case 85 => col("descArray")(x).alias("gfflag16")
          case 86 => col("descArray")(x).alias("gfflag17")
          case 87 => col("descArray")(x).alias("gfflag18")
          case 88 => col("descArray")(x).alias("gfflag19")
          case 89 => col("descArray")(x).alias("gfflag20")

          case 90 => col("descArray")(x).alias("gfflag21")
          case 91 => col("descArray")(x).alias("gfflag22")
          case 92 => col("descArray")(x).alias("gfflag23")
          case 93 => col("descArray")(x).alias("gfflag24")
          case 94 => col("descArray")(x).alias("gfflag25")
          case 95 => col("descArray")(x).alias("gfflag26")
          case 96 => col("descArray")(x).alias("gfflag27")
          case 97 => col("descArray")(x).alias("gfflag28")
          case 98 => col("descArray")(x).alias("gfflag29")
          case 99 => col("descArray")(x).alias("gfflag30")

          case 100 => col("descArray")(x).alias("gfflag31")
          case 101 => col("descArray")(x).alias("gfflag32")
          case 102 => col("descArray")(x).alias("gfflag33")
          case 103 => col("descArray")(x).alias("gfflag34")
          case 104 => col("descArray")(x).alias("gfflag35")
          case 105 => col("descArray")(x).alias("vslsessid")
          case 106 => col("descArray")(x).alias("vslconntxns")
          case 107 => col("descArray")(x).alias("vslmedtxns")
          case 108 => col("descArray")(x).alias("vslnrdntxns")
          case 109 => col("descArray")(x).alias("vslsessflag")

          case 110 => col("descArray")(x).alias("vslhrzres")
          case 111 => col("descArray")(x).alias("vslvertres")
          case 112 => col("descArray")(x).alias("vslfrmsze")
          case 113 => col("descArray")(x).alias("vslfrmrte")
          case 114 => col("descArray")(x).alias("vslmedtm")
          case 115 => col("descArray")(x).alias("vslsapreqb")
          case 116 => col("descArray")(x).alias("vslsaprspb")
          case 117 => col("descArray")(x).alias("vslpcrte")
          case 118 => col("descArray")(x).alias("vslstarttstmp")
          case 119 => col("descArray")(x).alias("httpmethod")

          case 120 => col("descArray")(x).alias("domain")
          case 121 => col("descArray")(x).alias("url")
          case 122 => col("descArray")(x).alias("query")
          case 123 => col("descArray")(x).alias("urlfcid")
          case 124 => col("descArray")(x).alias("urlfcgid")
          case 125 => col("descArray")(x).alias("urlfcrep")
          case 126 => col("descArray")(x).alias("urlfrspa")
          case 127 => col("descArray")(x).alias("urlfstmtc")
          case 128 => col("descArray")(x).alias("urlfstprv")
          //Missing
          case 129 => col("descArray")(x).alias("matchedurlset")

          case 130 => col("descArray")(x).alias("windowscalingenabled")
          case 131 => col("descArray")(x).alias("sackenabled")
          case 132 => col("descArray")(x).alias("wsfactorsentbyns")
          case 133 => col("descArray")(x).alias("wsfactor")
          case 134 => col("descArray")(x).alias("endpointmode")
          case 135 => col("descArray")(x).alias("rstended")
          case 136 => col("descArray")(x).alias("mss")
          case 137 => col("descArray")(x).alias("nsobspid")
          case 138 => col("descArray")(x).alias("nsobsdid")
          case 139 => col("descArray")(x).alias("nsexppid")

          case 140 => col("descArray")(x).alias("nstrid")
          case 141 => col("descArray")(x).alias("nsvsn")
          case 142 => col("descArray")(x).alias("nsslastupdtstmp")
          case 143 => col("descArray")(x).alias("nsvstcpstarttstmp")
          case 144 => col("descArray")(x).alias("netlabel")
          //Missing
          case 145 => col("descArray")(x).alias("tcpprofile")

          case 146 => col("descArray")(x).alias("conglev")
          case 147 => col("descArray")(x).alias("conglevclass")
          case 148 => col("descArray")(x).alias("signalqual")
          case 149 => col("descArray")(x).alias("signalqualclass")

          case 150 => col("descArray")(x).alias("version")
          case 151 => col("descArray")(x).alias("pid")
          case 152 => col("descArray")(x).alias("rsize")
          case 153 => col("descArray")(x).alias("srsize")
          case 154 => col("descArray")(x).alias("cid")
          case 155 => col("descArray")(x).alias("originalsize")
          case 156 => col("descArray")(x).alias("optimisedsize")
          case 157 => col("descArray")(x).alias("compressionpercent")
          case 158 => col("descArray")(x).alias("cachevalidbytes")
          case 159 => col("descArray")(x).alias("cachestatuscode")

          case 160 => col("descArray")(x).alias("httpcode")
          case 161 => col("descArray")(x).alias("socketunreadsize")
          case 162 => col("descArray")(x).alias("socketunsentsize")
          case 163 => col("descArray")(x).alias("medialogstring")
          case 164 => col("descArray")(x).alias("src_flag")
          case 165 => col("descArray")(x).alias("src_tcpsl")
          case 166 => col("descArray")(x).alias("contenttype")
          //Missing
          case 167 => col("descArray")(x).alias("MSH")
          case 168 => col("descArray")(x).alias("sessionid")
          case 169 => col("descArray")(x).alias("susbcriberid")

          case 170 => col("descArray")(x).alias("useragent")
          case 171 => col("descArray")(x).alias("deviceid")
          case 172 => col("descArray")(x).alias("uagroup")
          case 173 => col("descArray")(x).alias("catid")
          case 174 => col("descArray")(x).alias("httpref")
          case 175 => col("descArray")(x).alias("customrepgrp")
          case 176 => col("descArray")(x).alias("customrepopt1")
          case 177 => col("descArray")(x).alias("customrepopt2")
          case 178 => col("descArray")(x).alias("applicationid")
          case 179 => col("descArray")(x).alias("uxichannelbandwidth")

          case 180 => col("descArray")(x).alias("cellid")
          case 181 => col("descArray")(x).alias("cellcongestionlevel")
          case 182 => col("descArray")(x).alias("optsource")
          case 183 => col("descArray")(x).alias("predicteduagroup")
          case 184 => col("descArray")(x).alias("sslinfolog")

        }
    ):_*).drop("nonlkey_cols").drop("descArray")
    df2.printSchema()
    val df3 = enrichTCPSL(df2)
    df3.select("minrtt","avgrtt","maxrtt","bdp","avgbif","maxbif","pktlossrate","pktretransrate").show

    val df4 = df3.withColumn("sizetag", when(col("optimisedsize") < 1000,"Tiny")
      .when(col("optimisedsize") >= 1000 && col("optimisedsize") < 200000,"Small")
    .when(col("optimisedsize") >= 200000 && col("optimisedsize") < 1000000,"Medium")
    .when(col("optimisedsize") >= 1000000 && col("optimisedsize") < 5000000,"Large")
    .when(col("optimisedsize") >= 5000000, "Huge").otherwise("-"))
      .withColumn("flag", when(col("src_flag").equalTo(0),"NS_MEDIA_TYPE_UNCATEGORIZED")
        .when(col("src_flag").equalTo(29),"NS_MEDIA_TYPE_ENC_ABR")
        .when(col("src_flag").equalTo(30),"NS_MEDIA_TYPE_CT_PD")
        .when(col("src_flag").equalTo(31),"NS_MEDIA_TYPE_CT_ABR")
        .when(col("src_flag").equalTo(32),"NS_MEDIA_TYPE_OTHER")
        .when(col("src_flag").equalTo(33),"NS_MEDIA_TYPE_QUIC_ABR").otherwise("-"))

    df4.select("optimisedsize","sizetag","src_flag","flag").show
    df4





  }

  def enrichTCPSL(df:DataFrame): DataFrame ={
    df.withColumn("minrtt", split(col("src_tcpsl"),"/")(0))
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



}
