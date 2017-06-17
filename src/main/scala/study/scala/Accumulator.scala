package study.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Accumulator {
  def main(args: Array[String]) {
    var conf = new SparkConf().setAppName("Transformation ").setMaster("local")
    var sc = new SparkContext(conf)
    var accumulatorTmp = sc.longAccumulator("My Accumulator")
    var numbers = Array(1, 2, 3, 4, 5, 6)
    var numberRdd = sc.parallelize(numbers, 1)

    numberRdd.foreach(num ⇒ accumulatorTmp.add(num))
    println(accumulatorTmp.value)
    println(accumulatorTmp.value)
    println(accumulatorTmp.value)
    println("test")
    
    
  }
}
