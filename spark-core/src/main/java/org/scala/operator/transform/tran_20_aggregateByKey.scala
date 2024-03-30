package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子 aggregateByKey
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_20_aggregateByKey {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 6), ("a", 1), ("b", 3),("a", 4)),3)
    //分区内取出最大的，分区间相加
    //实现(a,【l,2】)，(a, 【6,1】)，(a, 【3,4】)
    // (a,2),(a,6),(a,4)
    //(a,12)
    //存在函数柯里化，有两个参数列表
    //第一个参数列表
    //第二个参数列表
    //  第一个参数表示分区内计算规则
    //  第二个参数表示分区间计算规则
    rdd.aggregateByKey(0)((x,y)=>{math.max(x,y)},_+_).collect().foreach(println)

    sc.stop()
  }
}
