package study.trex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TransformationJoin {
  def main(args: Array[String]) {
    var conf = new SparkConf().setAppName("TransformationGroupBykey ").setMaster("local")
    var sc = new SparkContext(conf)

    var scores = Array(Tuple2(80, 12), Tuple2(90, 17), Tuple2(20, 88), Tuple2(86, 21))
    var names = Array(Tuple2(80, "class2"), Tuple2(90, "class1"), Tuple2(20, "class1"), Tuple2(86, "class2"))

    var scoresRdd = sc.parallelize(scores, 1)
    var nameRdd = sc.parallelize(names, 1)
    
    var joinRdd=scoresRdd.join(nameRdd).sortByKey(true, 1)

    joinRdd.foreach(num ⇒ {
      println("class:" + num._1+ " score is:" + num._2._1+" name is:" + num._2._2)
      println("--------------------")
    })

  }
}