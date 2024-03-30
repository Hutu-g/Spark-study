package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * 转换算子 reduceByKey
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_18_reduceByKey {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 1), ("a", 6), ("d", 1), ("a", 3)))
//    相同的的key进行value聚合操作
    val reduceRdd: RDD[(String, Int)] = rdd.reduceByKey(_ + _,3)
    reduceRdd.collect().foreach(println)

    sc.stop()
  }
}
