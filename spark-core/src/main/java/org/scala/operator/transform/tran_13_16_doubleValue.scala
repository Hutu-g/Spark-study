package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子 双value类型
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_13_16_doubleValue {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))

    //交
    val interRdd: RDD[Int] = rdd1.intersection(rdd2)
    println(interRdd.collect().mkString(","))
    //并
    val unionRdd: RDD[Int] = rdd1.union(rdd2)
    println(unionRdd.collect().mkString(","))

    //差
    val subRdd: RDD[Int] = rdd1.subtract(rdd2)
    println(subRdd.collect().mkString(","))

    //拉链
    val zipRdd: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(zipRdd.collect().mkString(","))

//    sortRdd.saveAsTextFile("output/sortBy")
    sc.stop()
  }
}
