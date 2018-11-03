package com.johngodoi.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Reduce extends App {

  val conf = new SparkConf().setAppName("initial").setMaster("local")
  val sc = new SparkContext(conf)
  sc.parallelize(List(0,1,2,3,4,5,6,7,8,9)).reduce((x,y)=>x+y)
  sc.stop()
}
