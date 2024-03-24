package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子 simple
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_08_simple {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9,10))

    rdd.sample(
      true,
      2,
      1
    ).collect().foreach(println)
//    filterRdd.collect().foreach(println)
    sc.stop()
  }

}
