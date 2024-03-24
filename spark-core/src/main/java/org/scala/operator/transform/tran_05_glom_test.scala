package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_05_glom_test {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    val glomRdd: RDD[Array[Int]] = rdd.glom()

    val maxRdd: RDD[Int] = glomRdd.map(_.max)
    val sum: Int = maxRdd.collect().sum
    println(sum)
    sc.stop()
  }

}
