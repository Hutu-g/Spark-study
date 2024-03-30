package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子 foldByKey
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_21_foldByKey {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 6), ("a", 1), ("a", 3),("a", 4)),3)
    //分区内，分区间相加
    rdd.foldByKey(0)(_+_).collect().foreach(println)

    sc.stop()
  }
}
