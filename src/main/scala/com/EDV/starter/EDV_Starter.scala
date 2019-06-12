package com.EDV.starter

import com.EDV.EDV_Driver
import com.EDV.file_read.EDV_Param_Files
import org.apache.spark.sql.SparkSession

object EDV_Starter extends App with EDV_Param_Files {

  //val InputPath = args(0)
  //val OutputPath = args(1)
  //val Table = args(2)
  val Param_Src_File_InputPath = args(0) // fileinput = /user/app/file_q2.txt --> method1
  val Param_Tgt_Tbl_OutputPath = args(1)
  val Param_Tgt_File_OutputPath = args(2)
  val spark = SparkSession.builder().appName("EDV").enableHiveSupport().getOrCreate()
  val srcFileInput = paramFileCall(spark, Param_Src_File_InputPath)
  val tgtTableOutput = paramFileCall(spark, Param_Tgt_Tbl_OutputPath)
  val tgtFileOutput = paramFileCall(spark, Param_Tgt_File_OutputPath)
  val zipList = srcFileInput.zip((tgtTableOutput).zip(tgtFileOutput)).toMap //Map("inputfile" -> ("tablename1", "ouputfile1"), ("inpufile2" -> ("tablename2", "outputfile2"))
  zipList.foreach{case(fileInput, outputs) => EDV_Driver.initProcess(spark, fileInput, outputs._2, outputs._1)}
  //val zipList = srcFileInput.zip(tgtTableOutput).toMap //Map("inputfile" -> "tablename1", "inpufile2" -> "tablename2")
  //zipList.foreach{case(fileInput, tableName) => EDV_Driver.initProcess(spark, fileInput, OutputPath, tableName)}

}
