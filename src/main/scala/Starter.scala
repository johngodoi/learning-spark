import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jgodoi on 5/11/2017.
  */
object Starter extends App {

  val conf = new SparkConf().setAppName("workdCount").setMaster("local")
  val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
  val nums = sc.parallelize(List(1,2,3,4))
  val squared = nums.map(x=>x*x)

  println(squared.collect().mkString(","))
  val file = sc.textFile("build.sbt")
  println(file.filter(x => !x.isEmpty).filter(x => x.contains(":=")).map(x=>x.split(":=")(1).trim).collect().mkString(","))
}
