package org.scala.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @Author: hutu
 * @Date: 2024/2/29 18:05
 */
object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    //连接
    val wordCount: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(wordCount)

    //业务
    //1.读取文件
    //
    val lines: RDD[String] = sc.textFile("datas")
    //扁平化操作
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //格式化
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))
    //计数
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    //聚合
    val array: Array[(String, Int)] = wordToCount.collect()

    array.foreach(println)
    //关闭连接
    sc.stop();
  }

}
