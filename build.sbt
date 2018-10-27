name := "learning-spark"
version := "1.0"
scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1",
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "com.crealytics" %% "spark-excel" % "0.8.3"
)
        