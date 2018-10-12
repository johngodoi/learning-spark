package com.johngodoi.spark

import org.apache.spark.{SparkConf, SparkContext}

object Map extends App {
  val conf = new SparkConf().setAppName("initial").setMaster("local")
  val sc = new SparkContext(conf)
  val nums = sc.parallelize(List(1,2,3,4))

  val squared = nums.map(x=>x*x)
  println(squared.sum())
  sc.stop()
}
