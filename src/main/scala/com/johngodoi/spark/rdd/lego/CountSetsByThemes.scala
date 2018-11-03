package com.johngodoi.spark.rdd.lego

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CountSetsByThemes extends App {
  private val sparkConf = new SparkConf().setAppName("count sets by themes").setMaster("local[*]")
  private val sparkContext = new SparkContext(sparkConf)
  private val themes: RDD[(String, String)] = sparkContext.textFile("./src/main/resources/lego/themes.csv")
    .map(line => line.split(","))
    .map(values => (values(0), values(1)))

  sparkContext.textFile("./src/main/resources/lego/sets.csv")
    .map(line => line.split(","))
    .map(values  => values(3))
    .map(value => (value,1))
    .reduceByKey((v1,v2)=>v1+v2)
    .sortBy(_._2.toInt)
    .join(themes)
    .foreach(println(_))
  sparkContext.stop()
}
