package org.scala.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  创建rdd
 * @Author: hutu
 * @Date: 2024/3/23 11:55
 */
object buildRDD {
  /**
   * 从内存中创建rdd
   * @param args
   */
//  def main(args: Array[String]): Unit = {
//
//    //环境
//    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
//    val sc: SparkContext = new SparkContext(sparkConf)
//
//    //创建rdd
//    //从内存中创建
//    val seq = Seq[Int](1,2,3,4)
//    //parallelize:并行
//    //val rdd: RDD[Int] = sc.parallelize(seq)
//    //makeRdd() == parallelize()
//    val rdd: RDD[Int] = sc.makeRDD(seq)
//
//
//    rdd.collect().foreach(println)
//
//    //关闭
//    sc.stop()
//  }
  /**
   * 从文件中创建rdd
   *
   * @param args
   */
//  def main(args: Array[String]): Unit = {
//        //环境
//        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
//        val sc: SparkContext = new SparkContext(sparkConf)
//        //创建rdd
//        //从文件中创建
//        val rdd: RDD[String] = sc.textFile("datas")
//        rdd.collect().foreach(println)
//        //关闭
//        sc.stop()
//  }


  /**
   * 从目录中创建rdd
   */

  def main(args: Array[String]): Unit = {
    //环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc: SparkContext = new SparkContext(sparkConf)
    //创建rdd
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("datas")
    rdd.collect().foreach(println)
    //关闭
    sc.stop()
  }

}
