package com.johngodoi.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Cache extends App {
  val conf = new SparkConf().setAppName("initial").setMaster("local")
  val sc = new SparkContext(conf)
  private val nums: RDD[Int] = sc.parallelize(List(1,2,3,4))

  val squared = nums.map(x=>x*x)
  private val cached: RDD[Int] = squared.cache()
  println(cached.sum())
  println(cached.collect().mkString(","))
  sc.stop()
}
