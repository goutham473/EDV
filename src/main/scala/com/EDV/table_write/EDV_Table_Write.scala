package com.EDV.table_write

import org.apache.spark.sql.{DataFrame, SparkSession}

trait EDV_Table_Write {
  def Table_Write(spark: SparkSession, DF: DataFrame, tableName: String): Unit = {
    DF.createOrReplaceTempView("TableFile1")
    spark.sql(s"insert into table dpl.$tableName select * from TableFile1")
  }
}
