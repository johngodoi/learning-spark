package com.johngodoi.spark.rdd.lego

import com.johngodoi.spark.utils.Utils
import org.apache.spark.{SparkConf, SparkContext}

object CountSetsByYear extends App {

  private val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("count sets by year")
  private val sparkContext = new SparkContext(sparkConf)
  sparkContext.textFile("./src/main/resources/lego/sets.csv")
    .map(line => line.split(","))
    .map(values => values(2))
    .filter(value => Utils.isNumeric(value))
    .groupBy(value => value)
    .mapValues(occurrences => occurrences.size)
    .map(pair => (pair._1.toInt,pair._2))
    .sortByKey()
    .foreach(println(_))
  sparkContext.stop()
}
