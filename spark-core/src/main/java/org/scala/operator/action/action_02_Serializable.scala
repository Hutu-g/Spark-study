package org.scala.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 序列化
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object action_02_Serializable {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc: SparkContext = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))
    val search: Search = new Search("h")
    search.getMatch1(rdd).collect().foreach(println)


    sc.stop()
  }
  class Search(query:String) extends Serializable {
    //查询对象
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }
    //函数序列化
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(this.isMatch)
//      rdd.filter(isMatch)
    }
    //属性序列化
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      val q = query
      rdd.filter(x => x.contains(q))
    }
  }
}
