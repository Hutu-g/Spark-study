package org.scala.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @Author: hutu
 * @Date: 2024/2/29 18:05
 */
object Spark02_WordCount {
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

    val wordToOne: RDD[(String, Int)] = words.map(
      word => (word, 1)
    )

    //分组
    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne .groupBy(
      t => t._1
    )

    val wordToCount =  wordGroup.map{
        case (word,list) =>{
          list.reduce(
            (t1, t2) => {
              (t1._1, t1._2 + t2._2)
            }
          )
      }
    }

    val array: Array[(String, Int)] = wordToCount.collect()

    array.foreach(println)
    //关闭连接
    sc.stop();
  }

}
