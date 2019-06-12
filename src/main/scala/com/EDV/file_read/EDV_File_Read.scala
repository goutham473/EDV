package com.EDV.file_read

import org.apache.spark.sql.SparkSession
import scala.util.parsing.json.JSON

trait EDV_File_Read {
  def textFileRead(spark: SparkSession, InputPath: String) = spark.read.textFile(InputPath)
}
