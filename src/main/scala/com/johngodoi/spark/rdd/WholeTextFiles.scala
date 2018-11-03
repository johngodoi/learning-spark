package com.johngodoi.spark.rdd

object WholeTextFiles extends App {
  val conf = new SparkConf().setAppName("initial").setMaster("local")
  val sc = new SparkContext(conf)
  sc.setLogLevel("OFF")
  val wholeTextFiles = sc.wholeTextFiles("./src/main/resources/lego/colors*")
  //file:path,first line
  //another lines
  wholeTextFiles.foreach(file => println(s"${file._1},${file._2}"))
  sc.stop()

}
