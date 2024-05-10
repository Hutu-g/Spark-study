package org.scala.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 序列化
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object action_02_kray {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
                    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                    .registerKryoClasses(Array(classOf[Searcher]))


    val sc: SparkContext = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))
    val search: Searcher = new Searcher("hello")
    search.getMatchedRDD1(rdd).collect().foreach(println)
    

    sc.stop()
  }
  class Searcher(query:String) extends Serializable {
    //查询对象
    def isMatch(s: String) = {
      s.contains(query)
    }
    //函数序列化
    def getMatchedRDD1(rdd: RDD[String]) = {
      rdd.filter(isMatch)
    }
    //属性序列化
    def getMatchedRDD2(rdd: RDD[String]) = {
      val q = query
      rdd.filter(_.contains(q))
    }
  }
}
