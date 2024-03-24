package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子 groupBy
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_06_groupBy {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    val groupRdd: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)
    groupRdd.collect().foreach(println)
    sc.stop()
  }

}
