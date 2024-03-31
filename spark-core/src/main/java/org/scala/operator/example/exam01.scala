package org.scala.operator.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 案例 统计出每一个省份每个广告被点击数量排行的 Top3
 * 时间戳，省份，城市，用户，广告
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object exam01 {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("datas/agent.log")
    //结构转换
    val mapRdd: RDD[((String, String), Int)] = rdd.map(
      line => {
        val lineArr: Array[String] = line.split(" ")
        ((lineArr(1), lineArr(4)), 1)
      })
    //分组聚合
    val reduceRdd: RDD[((String, String), Int)] = mapRdd.reduceByKey(_ + _)
    //将聚合结果结构转换
    val newMapRdd: RDD[(String, (String, Int))] = reduceRdd.map {
      case ((pra, ad), sum) =>
        (pra, (ad, sum))
    }
    //分组排序
    val resRdd: RDD[(String, List[(String, Int)])] = newMapRdd.groupByKey().mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      })
    resRdd.collect().foreach(println)
    sc.stop()
  }
}
