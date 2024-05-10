package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}

/**
 *
 * @Author: hutu
 * @Date: 2024/5/8 10:05
 */
object Spark_sql_UDAF1 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate() //导入隐式转换 对象里的内容
    val df: DataFrame = spark.read.json("datas/user.json")

    df.createOrReplaceTempView("user")
    //给名称加个前缀
    spark.udf.register("ageAvg",functions.udaf(new MyAvgUDAF))
    spark.sql("select ageAvg(age) from user").show()

    //todo 关闭环境
    spark.close()


  }

  /**
   * 自定义聚合两数类:计算年龄的平均值
   *  继承org.apache.spark.sql.expressions 定义泛型  IN BUF OUT
   *  重写方法6
   */
  case class Buff(var total:Long,var count:Long)
  class MyAvgUDAF extends  Aggregator[Long,Buff,Long]{
    //初始值 或 0值
    //缓冲区初始化
    override def zero: Buff = {
      Buff(0L,0L)
    }
    //根据输入更新缓冲区数据
    override def reduce(buff: Buff, in: Long): Buff = {
      buff.total = buff.total + in
      buff.count = buff.count + 1
      buff
    }
    //合并缓冲区
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total = buff1.total + buff2.total
      buff1.count = buff1.count + buff2.count
      buff1
    }
    //计算结果
    override def finish(buff: Buff): Long = {
      buff.total/buff.count
    }
    //缓冲区编码
    override def bufferEncoder: Encoder[Buff] = Encoders.product
    //输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
