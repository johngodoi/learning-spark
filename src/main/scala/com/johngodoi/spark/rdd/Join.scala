package com.johngodoi.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Join extends App {

  val conf = new SparkConf().setAppName("initial").setMaster("local")
  val sc = new SparkContext(conf)
  private val values: RDD[(Int, Int)] = sc.parallelize(List((1, 2), (2, 4), (2, 5), (2, 0), (1, 9)))
  sc.parallelize(List((1,2),(2,4))).join(values).foreach(p => println(p))
  sc.stop()
}
