package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_01 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc: SparkContext = new SparkContext(sparkConf)
    //1.map()


    //关闭
    sc.stop()
  }

}
