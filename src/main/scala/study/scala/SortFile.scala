package study.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SortFile {
  def main(args: Array[String]) {
    var conf = new SparkConf().setAppName("ParallelizeConnection").setMaster("local")
    var sc = new SparkContext(conf)
    val lines = sc.textFile("/Users/oushuhua/LICENSE-jline.txt", 1)
    var words = lines.flatMap(line ⇒ line.split(" "))
    var wordPair = words.map(word ⇒ (word, 1))
    var count = wordPair.reduceByKey(_ + _)
    var count2 = count.map(wordPair ⇒ (wordPair._2, wordPair._1)).sortByKey(false, 1)
    count = count2.map(wordPair ⇒ (wordPair._2, wordPair._1))
    count.foreach(wordPa ⇒ println("word:" + wordPa._1 + " count:" + wordPa._2))

  }
}