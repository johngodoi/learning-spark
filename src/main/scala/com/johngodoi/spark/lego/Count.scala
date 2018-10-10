package com.johngodoi.spark.lego

import org.apache.spark.sql.{DataFrame, SparkSession}

object Count extends App {
  private val sparkSession: SparkSession = SparkSession.builder.appName("count_lego").master("local[*]").getOrCreate()
  private val themesDF: DataFrame = sparkSession.read.option("header","true").csv("./src/main/resources/lego/themes.csv")
  themesDF.cache()
  println(themesDF.count())
  themesDF.show()
}
