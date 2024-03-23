package org.scala.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  分区与并行度
 *
 * @Author: hutu
 * @Date: 2024/3/23 12:22
 */
object par_rdd {
  /**
   * 集合分区
   */
  //  def main(args: Array[String]): Unit = {
//
//    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
//
//    val sc: SparkContext = new SparkContext(sparkConf)
//    //创建rdd
//    val rdd: RDD[Int] = sc.makeRDD(
//      List(1, 2, 3, 4)
//    )
//     //将处理的数据保存成分区文件
//    rdd.saveAsTextFile("output")
//
//
//    //关闭
//    sc.stop()
//  }


  /**
   * 文件分区
   */
    def main(args: Array[String]): Unit = {

      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

      val sc: SparkContext = new SparkContext(sparkConf)
      //创建rdd
      val rdd: RDD[String] = sc.textFile("datas")

       //将处理的数据保存成分区文件
      rdd.saveAsTextFile("output")


      //关闭
      sc.stop()
    }
}
