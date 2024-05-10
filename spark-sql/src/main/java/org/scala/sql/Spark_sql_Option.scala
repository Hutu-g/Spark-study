package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 *
 * @Author: hutu
 * @Date: 2024/5/8 10:05
 */
object Spark_sql_Option {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate() //导入隐式转换 对象里的内容
    //通用
//    val df: DataFrame = spark.read.load("datas/example/users.parquet")
//    df.show()
//    df.write.save("output/example/usersParquet")
    //json & csv
    val df: DataFrame = spark.read.json("datas/user.json")
    spark.sql("select * from json.`datas/user.json`").show
//    df.write.format("json").save("output/example/jsonData")
    df.write.format("json").mode("overwrite").save("output/example/jsonData")
    val df2: DataFrame = spark.read.option("seq", ";").option("header", "true").csv("datas/example/people.csv")
    df2.show()
    //mysql
    val df3: DataFrame =  spark.read
                          .format("jdbc")
                          .option("url", "jdbc:mysql://localhost:3306/spider_db")
                          .option("user", "root")
                          .option("password", "123456")
                          .option("dbtable", "top250")
                          .load()
    df3.show()
    df3.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spider_db")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "top250_spark_sql")
      .mode(SaveMode.Append)
      .save("output/example/mysqlData")
    //



    spark.close()


  }

}
