package com.dl.deltatable

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import io.delta.tables._
import org.apache.spark.sql.functions.{col, lit}

object DeltaLakeOperations {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.master("local[*]").appName("Write dataframe to Redshift Example").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val deltalakeConfig = rootConfig.getConfig("deltalake_conf")

    println("\nCreating a new table \"sample-data-table\"")

    val data = sparkSession.range(1,5)
    data.printSchema()
    data.show()
    data
      .coalesce(1)
      .write.format("delta")
      .mode("overwrite")
      .save( deltalakeConfig.getString("delta_table_path"))

    val fs = FileSystem.get(new Configuration())
    var status = fs.listStatus(new Path( deltalakeConfig.getString("delta_table_path")))
    status.foreach(x=> println(x.getPath))

    status = fs.listStatus(new Path( deltalakeConfig.getString("delta_table_path")+ "/_delta_log/"))
    status.foreach(x=> println(x.getPath))

    //Fails while appending
    //Exception in thread "main" org.apache.spark.sql.AnalysisException: Failed to merge fields 'id' and 'id'. Failed to merge incompatible data types LongType and StringType;;
    /*
    val df_new_data = sparkSession.range(5,10)
      .withColumn("id", col("id").cast("String"))
    df_new_data.printSchema()
    df_new_data.show()
    df_new_data
      .coalesce(1)
      .write.format("delta")
      .mode("append").save(deltalakeConfig.getString("delta_table_path"))
    */
    println("\nINSERT rows into table  \"sample-data-table\"")
    val new_data = sparkSession.range(5,10)
    new_data
      .coalesce(1)
      .write.format("delta")
      .mode("append").save(deltalakeConfig.getString("delta_table_path"))

    status = fs.listStatus(new Path( deltalakeConfig.getString("delta_table_path")))
    status.foreach(x=> println(x.getPath))

    status = fs.listStatus(new Path( deltalakeConfig.getString("delta_table_path")+ "/_delta_log/"))
    status.foreach(x=> println(x.getPath))

    val sampleDeltaTable = DeltaTable.forPath(sparkSession, deltalakeConfig.getString("delta_table_path"))

    // Delta tables aren't DataFrame, we cannot perform spark dataframe operations on it,
    // but that can be achieved by converting it into Spark Dataframe through delta_df.toDF() operation.
    println("\nREAD table  \"sample-data-table\"")
    println("\nOption 1:")
    sampleDeltaTable.toDF.show()
    println("\nOption 2:")
    val DeltaTableDF = sparkSession.read.format("delta").load(deltalakeConfig.getString("delta_table_path"))
    DeltaTableDF.show()


    println("\nDELETE rows in table \"sample-data-table\"")
    sampleDeltaTable.delete("id <= 2")
    sampleDeltaTable.toDF.show()

    status = fs.listStatus(new Path( deltalakeConfig.getString("delta_table_path")))
    status.foreach(x=> println(x.getPath))

    status = fs.listStatus(new Path( deltalakeConfig.getString("delta_table_path")+ "/_delta_log/"))
    status.foreach(x=> println(x.getPath))

    /*
    println("\nREAD delta log file")
    val lines = sparkSession.sparkContext.textFile("/deltalake-demo/tables/sample-delta-table/_delta_log/00000000000000000002.json")
    lines.foreach(println)
    */

    println("\nUPDATE rows in table \"sample-data-table\"")
    sampleDeltaTable.update(col("id") === "5", Map("id" -> lit("500")))
    sampleDeltaTable.toDF.show()

    println("\nUPSERT(MERGE) data rows in table \"delta-merge-table\"")
    import sparkSession.sqlContext.implicits._

    val df1 = List(("Peru", 2011, 22.029), ("India", 2006, 24.73)).toDF("country", "year", "temperature")
    val df2 = List(("Australia", 2019, 50.0), ("India", 2010, 100.0)).toDF("country", "year", "temperature")

    df1
      .write.format("delta")
      .mode("overwrite")
      .save( deltalakeConfig.getString("delta_merge_table_path"))

    val mergeDeltaTable = DeltaTable.forPath(sparkSession, deltalakeConfig.getString("delta_merge_table_path"))

    println("\nBefore MERGE")

    mergeDeltaTable.toDF.show()
    // Prior to Databricks Runtime 6.6, INSERT action required all columns in the target table to be provided.
    mergeDeltaTable
      .as("targetTable")
      .merge(
        df2.as("newData"),
        "targetTable.country = newData.country")
      .whenMatched
      .updateExpr(
        Map( "year" -> "newData.year",
          "temperature" -> "newData.temperature"))
      .whenNotMatched
      .insertExpr(
        Map(
          "country" -> "newData.country",
          "year" -> "newData.year",
          "temperature" -> "newData.temperature"))
      .execute()

    println("\nAfter MERGE")
    mergeDeltaTable.toDF.show()

    println("\nTIME TRAVEL in table \"delta-merge-table\"")

    mergeDeltaTable.history().show()// get the full history of the table
 //   mergeDeltaTable.history(1).show()// get the last operation

    val version_0 = sparkSession.read.format("delta").option("versionAsOf", 0).load( deltalakeConfig.getString("delta_merge_table_path"))
    version_0.show()


   // mergeDeltaTable.vacuum()        // vacuum files not required by versions older than the default retention period (7 days)
    // To set in spark session level:     sparkSession.config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    //  mergeDeltaTable.vacuum(100)     // vacuum files not required by versions more than 100 hours old
    // mergeDeltaTable.vacuum( 0.000001)

    println("\nALTER(add column) table \"delta-merge-table\"")

    println("\nBefore ALTER")
    mergeDeltaTable.toDF.show()

    mergeDeltaTable
      .toDF
      .withColumn("isOffical",lit("No"))
      .write.format("delta")
      .option("mergeSchema", "true")
      .mode("overwrite")
      .save( deltalakeConfig.getString("delta_merge_table_path"))

    println("\nAfter ALTER")
    DeltaTable.forPath(sparkSession, deltalakeConfig.getString("delta_merge_table_path")).toDF.show()

    /*
            Exception in thread "main" org.apache.spark.sql.AnalysisException: A schema mismatch detected when writing to the Delta table.
        To enable schema migration, please set:
        '.option("mergeSchema", "true")'.

        Table schema:
        root
        -- country: string (nullable = true)
        -- year: integer (nullable = true)
        -- temperature: double (nullable = true)


        Data schema:
        root
        -- country: string (nullable = true)
        -- year: integer (nullable = true)
        -- temperature: double (nullable = true)
        -- isOffical: string (nullable = true)


        To overwrite your schema or change partitioning, please set:
        '.option("overwriteSchema", "true")'.

        Note that the schema can't be overwritten when using
        'replaceWhere'.

        If Table ACLs are enabled, these options will be ignored. Please use the ALTER TABLE
        command for changing the schema.
     */
  }

}
