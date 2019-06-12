package com.EDV.file_read

import org.apache.spark.sql.SparkSession

trait EDV_Param_Files extends EDV_File_Read {

  def paramFileCall(spark: SparkSession, Param_InputPath: String) = {

    val paramFileRead = textFileRead(spark: SparkSession, Param_InputPath: String)
    paramFileRead.collect().toList
  }
}
