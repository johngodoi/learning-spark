package com.johngodoi.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object ParallelizePairs extends App {

  val conf = new SparkConf().setAppName("initial").setMaster("local")
  val sc = new SparkContext(conf)
  sc.parallelize(List((1,2),(2,4))).mapValues(v => v*v).foreach(p => println(p))
  sc.stop()
}
