package org.scala.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @Author: hutu
 * @Date: 2024/2/29 18:05
 */
object Test_Phone_Count {
  def main(args: Array[String]): Unit = {
    //连接
    val wordCount: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(wordCount)

    //业务
    //1.读取文件
    //
    val lines: RDD[String] = sc.textFile("datas/phone_data.txt")
    //格式化
    val kv: RDD[(String, Int)] = lines.map(x => {
      val strings: Array[String] = x.split("\\s+")
      (strings(1), strings(strings.length - 2).toInt)
    })
    val res: RDD[(String, Int)] = kv.reduceByKey(_ + _)
    res.collect().foreach(println)

    //关闭连接
    sc.stop();
  }

}
