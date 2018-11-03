package com.johngodoi.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Parallelize extends App {
  val conf = new SparkConf().setAppName("initial").setMaster("local")
  val sc = new SparkContext(conf)
  sc.parallelize(List(1,2,3,4)).foreach(n=>println(n))
  sc.stop()
}
