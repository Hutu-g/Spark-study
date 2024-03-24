package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子 sortBy
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_12_sortBy_test {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 2), ("11", 2), ("2", 2), ("4", 2)), 2)
    val sortRdd: RDD[(String, Int)] = rdd.sortBy(t => t._1.toInt)

    sortRdd.saveAsTextFile("output/sortBy_test")
    sc.stop()
  }
}
