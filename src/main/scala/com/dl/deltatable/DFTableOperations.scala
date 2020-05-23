package com.dl.deltatable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object DFTableOperations {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local[*]").appName("Write dataframe to Redshift Example").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val deltalakeConfig = rootConfig.getConfig("deltalake_conf")

    // Problem with DataFrame
    println("\nCreating a new table \"sample-data-table\"")
    val df_data= sparkSession.range(1,5)
    df_data.printSchema()
    df_data.show()
    df_data
      .coalesce(1)
      .write.format("parquet")
      .mode("overwrite")
      .save( deltalakeConfig.getString("hdfs_table_path"))

    val fs = FileSystem.get(new Configuration())
    var status = fs.listStatus(new Path( deltalakeConfig.getString("hdfs_table_path")))
    status.foreach(x=> println(x.getPath))

    println("\nAdd new data into table  \"sample-data-table\"")

    val df_new_data = sparkSession.range(5,10)
      .withColumn("id", col("id").cast("String"))
    df_new_data.printSchema()
    df_new_data.show()
    df_new_data
      .coalesce(1)
      .write.format("parquet")
      .mode("append").save(deltalakeConfig.getString("hdfs_table_path"))

    status = fs.listStatus(new Path( deltalakeConfig.getString("hdfs_table_path")))
    status.foreach(x=> println(x.getPath))

    // The problem starts reading
    // ERROR: org.apache.spark.sql.execution.QueryExecutionException: Parquet column cannot be converted in file file:///deltalake-demo/tables/sample-data-table/part-00000-f461a4f2-05b6-4f2e-b468-c12912b261b3-c000.snappy.parquet. Column: [id], Expected: bigint, Found: BINARY
//    val res_data = sparkSession.read.parquet(deltalakeConfig.getString("hdfs_table_path"))
//    res_data.printSchema()
//    res_data.show()
  }

}
