package study.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ParallelizeConnection {
  def main(args: Array[String]) {
    var conf = new SparkConf().setAppName("ParallelizeConnection").setMaster("local")
    var sc = new SparkContext(conf)
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    var numberRdd = sc.parallelize(numbers, 5)
    var sum = numberRdd.reduce(_ + _)
    
    println(sum)
  }
}