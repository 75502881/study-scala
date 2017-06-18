package study.trex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TransformationGroupBykey {
  def main(args: Array[String]) {
    var conf = new SparkConf().setAppName("TransformationGroupBykey ").setMaster("local")
    var sc = new SparkContext(conf)

    var scores = Array(Tuple2("class2", 80), Tuple2("class1", 90), Tuple2("class1", 20), Tuple2("class2", 85), Tuple2("class1", 30))

    var scoresRdd = sc.parallelize(scores, 1)
    var scoresGroupBy = scoresRdd.groupByKey();

    scoresGroupBy.foreach(num ⇒ {
      println("class:" + num._1)
      num._2.foreach(score ⇒ println("score is:" + score))
      
      println("--------------------")
    })

  }
}