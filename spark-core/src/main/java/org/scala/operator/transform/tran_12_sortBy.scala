package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子 sortBy
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_12_sortBy {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,5,98,1,23,4,5647,41,3,4),2)
    val sortRdd: RDD[Int] = rdd.sortBy(num => num)

    sortRdd.saveAsTextFile("output/sortBy")
    sc.stop()
  }
}
