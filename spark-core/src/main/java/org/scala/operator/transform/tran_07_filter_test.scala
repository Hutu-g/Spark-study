package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从服务器日志数据 apache.log 中获取 2015 年 5 月 17 日的请求路径
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_07_filter_test {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    val filterRdd: RDD[String] = rdd.filter(line => {
      val day: String = line.split(" ")(3).split(":")(0)
      day.startsWith("17/05/2015")
    })
    val url: RDD[String] = filterRdd.mapPartitions(_.map(_.split(" ")(6)))

    url.collect().foreach(println)
    sc.stop()
  }

}
