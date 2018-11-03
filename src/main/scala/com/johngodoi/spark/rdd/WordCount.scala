package com.johngodoi.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App {
  private val sparkConf = new SparkConf().setMaster("local[*]").setAppName("word count")
  private val sparkContext = new SparkContext(sparkConf)

  private val bloodstainPaper: RDD[String] = sparkContext.textFile("./src/main/resources/sps/John__SPS2011.txt")
  bloodstainPaper.filter(line => line.nonEmpty)
    .flatMap(line => line.split(" "))
    .groupBy(word => word)
    .mapValues(words => words.size)
    .sortBy(_._2,ascending = false)
    .foreach(println(_))

  sparkContext.stop()

}
