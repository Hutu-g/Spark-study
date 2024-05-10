package sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 *
 * @Author: hutu
 * @Date: 2024/5/8 10:05
 */
object Spark_sql_Basic {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._ //导入隐式转换 对象里的内容
    //todo DataFrame 是特定泛型的DataSet
//    val df: DataFrame = spark.read.json("datas/user.json")
//    df.show()
    //DataFrame => SQL
//    df.createTempView("user")
//    spark.sql("select * from user").show()
//    spark.sql("select age from user").show()
//    spark.sql("select avg(age) from user").show()
    //DataFrame => DSQ
//    df.select("age","username").show()
//    df.select("age").show()
//    df.select($"age"+1).show()


    //todo DataSet
//    val seq = Seq(1, 2, 3, 4)
//    val ds: Dataset[Int] = seq.toDS()
//
//    ds.show()

    //todo RDD  <=> DataFrame
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "张三", 20), (2, "李四", 18)))
    val df: DataFrame = rdd.toDF("id", "name", "age")
    val rowRDD: RDD[Row] = df.rdd

    //todo DataFrame  <=> DataSet
    val ds: Dataset[User] = df.as[User]
    val df1: DataFrame = ds.toDF()

    //todo RDD  <=> DataSet
    val ds1: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    val userRdd: RDD[User] = ds1.rdd


    //todo 关闭环境
    spark.close()


  }

  case class User(id:Int,name:String,age:Int)

}
