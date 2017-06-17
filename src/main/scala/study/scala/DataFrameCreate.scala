package study.scala

import org.apache.spark.sql.SparkSession

object DataFrameCreate {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val df = spark.read.json("hdfs://Hadoop-NameNode:9000/sparksql/students.json")
    df.show()
    df.printSchema()

    df.select("name").show()
    df.select(df.col("name"), df.col("age") + 100).show()

    df.filter(df.col("age") > 17).show()
    df.groupBy("age").count().show()
  }
}