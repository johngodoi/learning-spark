package com.johngodoi.spark.rdd.lego

import com.johngodoi.spark.utils.Utils
import org.apache.spark.{SparkConf, SparkContext}

object PartsQttByColor extends App {
  private val sparkConf: SparkConf  = new SparkConf().setMaster("local[*]").setAppName("parts qtt by color")
  private val sparkContext = new SparkContext(sparkConf)
  sparkContext.textFile("./src/main/resources/lego/inventory_parts.csv")
    .map(lines => lines.split(","))
    .groupBy(values => values(2))
    .mapValues(values=>values.map(value => value(3))
      .filter(value => Utils.isNumeric(value))
      .map(value => value.toInt).sum)
    .reduceByKey((v1,v2)=>v1+v2).foreach(println(_))
  sparkContext.stop()
}
