package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子mapPartitionsWithIndex
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_03_mapPartitionsWithIndex {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc: SparkContext = new SparkContext(sparkConf)
    //1.mapPartitionsWithIndex:获取第二个数据分区的数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    //获取第二个数据分区的数据
    val mapRdd: RDD[Int] = rdd.mapPartitionsWithIndex((index, iter) => {
      if (index == 1) {
        iter
      } else
        Nil.iterator
    })

    mapRdd.collect().foreach(println)
    sc.stop()
  }

}
