package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * 转换算子 partitionBy
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_17_partitionBy {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val mapRdd: RDD[(Int, Int)] = rdd.map((_, 1))
    mapRdd.partitionBy(new HashPartitioner(3)).saveAsTextFile("output/partitionBy02")

    sc.stop()
  }
}
