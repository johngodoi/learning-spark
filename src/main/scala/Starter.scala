import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jgodoi on 5/11/2017.
  */
object Starter extends App {

  val conf = new SparkConf().setAppName("wordCount").setMaster("local")
  val sc = new SparkContext(conf)
  sc.setLogLevel("OFF")
  val nums = sc.parallelize(List(1,2,3,4))
  val squared = nums.map(x=>x*x)
  private val cached: RDD[Int] = squared.cache()

  println(cached.sum())
  println(cached.collect().mkString(","))



  val file = sc.textFile("build.sbt")
  println(file.filter(x => !x.isEmpty).filter(x => x.contains(":=")).map(x=>x.split(":=")(1).trim).collect().mkString(","))

  private val sqlContext = new SQLContext(sc)

  def readExcel(file: String): DataFrame = sqlContext.read
    .format("com.crealytics.spark.excel")
    .option("location", file)
    .option("useHeader", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "false")
    .option("addColorColumns", "False")
    .load()

  val data = readExcel("path.xlsx")
  data.show(false)
}

