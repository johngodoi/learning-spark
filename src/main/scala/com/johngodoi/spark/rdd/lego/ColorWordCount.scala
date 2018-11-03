package com.johngodoi.spark.rdd.lego

import com.johngodoi.spark.utils.Utils
import org.apache.spark.{SparkConf, SparkContext}

object ColorWordCount extends App {

  val conf = new SparkConf().setAppName("initial").setMaster("local")
  val sc = new SparkContext(conf)
  val colors = sc.textFile("./src/main/resources/lego/colors.csv")
  colors.cache()
  colors.flatMap(line => line.split(","))
    .filter(value => !Utils.isNumeric(value))
    .flatMap(value => value.split(" "))
    .flatMap(value => value.split("-"))
    .groupBy((word:String)=>word)
    .mapValues(_.size)
    .filter(_._2>2)
    .sortByKey()
    .collect().foreach(println(_))

  sc.stop()
}
