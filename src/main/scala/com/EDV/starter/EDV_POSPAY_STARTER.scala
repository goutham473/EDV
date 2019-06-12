package com.EDV.starter

import com.EDV.EDV_Driver
import com.EDV.file_read.EDV_File_Read
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSON

object EDV_POSPAY_STARTER extends App with EDV_File_Read {


  val Json_Param_Input = args(0) // fileinput = /user/app/file_q2.txt --> method1
  val spark = SparkSession.builder().appName("EDV").enableHiveSupport().getOrCreate()
  EDV_Driver.driver_pospay(spark, Json_Param_Input)


}
