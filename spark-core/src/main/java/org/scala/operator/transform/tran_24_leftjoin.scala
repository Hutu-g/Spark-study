package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子 leftjoin 类似sql的左外连接
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_24_leftjoin {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 6)))
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 5), ("b", 5)))
    val leftJoinRdd: RDD[(String, (Int, Option[Int]))] = rdd.leftOuterJoin(rdd1)
    leftJoinRdd.collect().foreach(println)

    sc.stop()
  }
}
