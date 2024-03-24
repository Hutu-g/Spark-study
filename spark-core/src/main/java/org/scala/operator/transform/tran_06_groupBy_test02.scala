package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从服务器日志数据 apache.log 中获取每个时间段访问量。
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_06_groupBy_test02 {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    val timeRdd: RDD[(String, Iterable[(String, Int)])] = rdd.map(line => {
      val hour: String = line.split(" ")(3).split(":")(1)
      (hour, 1)
    }).groupBy(_._1)

    timeRdd.map{
      case (hour, iter) =>
        (hour,iter.size)
    }.collect().foreach(println)
    sc.stop()
  }

}
