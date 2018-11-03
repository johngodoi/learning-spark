package com.johngodoi.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object GroupByKey extends App {

  val conf = new SparkConf().setAppName("initial").setMaster("local")
  val sc = new SparkContext(conf)
  sc.parallelize(List((1,2),(2,4),(2,5),(2,0),(1,9))).groupByKey().mapValues(v => v.size).foreach(p => println(p))
  sc.stop()
}
