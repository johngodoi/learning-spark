package com.johngodoi.spark.rdd.lego

import com.johngodoi.spark.utils.Utils
import org.apache.spark.{SparkConf, SparkContext}

object CountPartsByCategory extends App {
  private val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("count parts by category")
  private val sparkContext = new SparkContext(sparkConf)
  sparkContext.textFile("./src/main/resources/lego/parts.csv")
    .map(line=>line.split(","))
    .map(values => values(values.size-1))
    .filter(cat => Utils.isNumeric(cat))
    .groupBy(cat => cat)
    .mapValues(occurrences => occurrences.size)
    .collect()
    .sortBy(pair => pair._1.toInt)
    .foreach(println(_))
  sparkContext.stop()
}
