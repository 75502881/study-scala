package study.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Transformation {
  def main(args: Array[String]) {
    var conf = new SparkConf().setAppName("Transformation ").setMaster("local")
    var sc = new SparkContext(conf)
    var numbers = Array(1, 2, 3, 4, 5, 6)
    var numberRdd = sc.parallelize(numbers, 1)

    var multNumberRdd = numberRdd.map(num ⇒ num * 2)
    multNumberRdd.foreach(num⇒println(num))
  }
}