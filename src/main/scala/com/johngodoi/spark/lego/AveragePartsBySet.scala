package com.johngodoi.spark.lego

import com.johngodoi.spark.utils.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AveragePartsBySet extends App {
  private val sparkConf = new SparkConf().setAppName("average parts by set").setMaster("local[*]")
  private val sparkContext = new SparkContext(sparkConf)
  private val mean: Double = sparkContext.textFile("./src/main/resources/lego/sets.csv")
    .map(line => line.split(","))
    .map(values => values(4))
    .filter(value => Utils.isNumeric(value))
    .map(value => value.toInt).mean()
  sparkContext.stop()
  println(mean)
}
