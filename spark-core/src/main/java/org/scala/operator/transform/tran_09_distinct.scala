package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子 distinct
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_09_distinct {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(5, 3, 1, 4, 5, 5, 5, 5, 5, 101, 2, 3, 4))

    rdd.distinct().collect().foreach(println)
    sc.stop()
  }

}
