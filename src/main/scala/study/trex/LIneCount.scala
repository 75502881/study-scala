package study.trex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object LineCount {
  def main(args: Array[String]) {
    var conf = new SparkConf().setAppName("ParallelizeConnection").setMaster("local")
    var sc = new SparkContext(conf)
    val lines=sc.textFile("/Users/oushuhua/test.txt",1)
    var pairs= lines.map(line⇒(line,1))
    var lineCounts=pairs.reduceByKey(_ + _)
    lineCounts.foreach(lineCount ⇒(println(lineCount._1 +" appears "+ lineCount._2)))
  }
}