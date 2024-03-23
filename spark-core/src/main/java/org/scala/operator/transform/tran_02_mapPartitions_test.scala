package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 获取每个数据分区的最大值
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_02_mapPartitions_test {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc: SparkContext = new SparkContext(sparkConf)
    //1.mapPartitions:
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    val mapRdd: RDD[Int] = rdd.mapPartitions(iter => {
      List(iter.max).iterator
    })
    mapRdd.collect().foreach(println)
    sc.stop()
  }

}
