package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子flatMap
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_04_flatMap {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))
    val flatMap: RDD[Int] = rdd.flatMap(list => {
      list
    })

    flatMap.collect().foreach(println)
    sc.stop()
  }

}
