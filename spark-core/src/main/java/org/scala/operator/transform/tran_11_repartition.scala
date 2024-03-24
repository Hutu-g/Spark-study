package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子 repartition
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_11_repartition {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 1)
    val newRdd: RDD[Int] = rdd.repartition(2)
    newRdd.saveAsTextFile("output/repartition")
    sc.stop()
  }
}
