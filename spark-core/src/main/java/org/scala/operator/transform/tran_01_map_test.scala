package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 获取文件中某一列
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_01_map_test {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc: SparkContext = new SparkContext(sparkConf)
    //1.map()
    val rdd: RDD[String] = sc.textFile("datas/apache.log")

//    val mapRdd: RDD[String] = rdd.map(line => {
//      val url: String = line.split(" ")(6)
//      url
//    })
    val mapRdd: RDD[String] = rdd.map(_.split(" ")(6))
    mapRdd.collect.foreach(println)

    sc.stop()
  }

}
