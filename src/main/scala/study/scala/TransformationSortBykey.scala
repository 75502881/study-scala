package study.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TransformationSortBykey {
  def main(args: Array[String]) {
    var conf = new SparkConf().setAppName("TransformationGroupBykey ").setMaster("local")
    var sc = new SparkContext(conf)

    var scores = Array(Tuple2( 80,"class2"), Tuple2(90,"class1"), Tuple2(20,"class1"), Tuple2(86,"class2"))

    var scoresRdd = sc.parallelize(scores, 1)
    var scoresGroupBy = scoresRdd.sortByKey(false, 1);

    scoresGroupBy.foreach(num ⇒ {
      println("class:" + num._2+" score is:" + num._1)
    })

  }
}