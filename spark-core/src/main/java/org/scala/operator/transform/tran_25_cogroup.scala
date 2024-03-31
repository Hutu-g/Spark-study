package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子 cogroup :connect + group
 * 例子
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_25_cogroup {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 6)))
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 5), ("b", 5),("b", 8)))
    val cogroupRdd: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd.cogroup(rdd1)
    cogroupRdd.collect().foreach(println)

    sc.stop()
  }
}
