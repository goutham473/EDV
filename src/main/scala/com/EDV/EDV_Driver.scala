package com.EDV

import com.EDV.process.{EDV_POSPAY_JSON_PROCESS, EDV_Positive_Pay_Process}
import com.EDV.starter.EDV_POSPAY_STARTER.{Json_Param_Input, spark, textFileRead}
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSON

object EDV_Driver {

  def initProcess(spark: SparkSession, inputPath: String, outputPath: String, tableName: String) = {
    EDV_Positive_Pay_Process.positive_pay(spark, inputPath, outputPath, tableName)
  }

  def driver_pospay(spark: SparkSession, json_Param_Input: String): Unit = {
    EDV_POSPAY_JSON_PROCESS.EDV_POSPAY_process(spark, Json_Param_Input )

  }

}
