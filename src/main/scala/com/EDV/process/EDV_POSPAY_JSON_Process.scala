package com.EDV.process

import com.EDV.file_read.EDV_File_Read
import com.EDV.file_write.EDV_File_Write
import com.EDV.process.EDV_Positive_Pay_Process.textFileRead
import com.EDV.starter.EDV_POSPAY_STARTER.{Json_Param_Input, textFileRead}
import com.EDV.table_write.EDV_Table_Write
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.parsing.json.JSON

case class Sample1File(bank_no: Int, bank_acct: Int, issue_date: String)
case class Sample2File(bank_no: Int, bank_acct: Int, issue_date: String, check_amt: Double, void_ind: String, first_name: String, last_name: String)
case class Sample3File(bank_no: Int, bank_acct: Int, issue_date: String, check_amt: Double, void_ind: String, first_name: String, last_name: String, void_adden: String)
object EDV_POSPAY_JSON_PROCESS extends EDV_File_Read with EDV_File_Write with EDV_Table_Write {

  def EDV_POSPAY_process(spark: SparkSession, Json_Param_Input: String ): Unit = {
    val jsonParam = textFileRead(spark: SparkSession, Json_Param_Input: String)

    val jsonMap = JSON.parseFull(jsonParam.collect().mkString(" ")).get.asInstanceOf[Map[String, Any]]

    val parseJsonInputFiles = jsonMap("InputFiles").asInstanceOf[List[Map[String, String]]](0)
    val parseJsonOutputFiles = jsonMap("OutputFiles").asInstanceOf[List[Map[String, String]]](0)
    val parseJsonTables = jsonMap("Tables").asInstanceOf[List[Map[String, String]]](0)

    val keysTuple = parseJsonInputFiles.keys.zip(parseJsonOutputFiles.keys).zip(parseJsonTables.keys)

    val JsonKeys = keysTuple.map(a => (a._1._1, a._1._2, a._2)).toList

    val mapValues = parseJsonInputFiles.values.zip(parseJsonOutputFiles.values).zip(parseJsonTables.values)

    val JsonValues = mapValues.map(a => (a._1._1,a._1._2,a._2)).toList

    val paramset = JsonKeys.zip(JsonValues)
paramset.foreach(k => k._1 match {
  case("InputFile1","OutputFile1","Table1") => SampleFile1(k._2._1,k._2._2, k._2._3)
  case("InputFile2","OutputFile2","Table2") => SampleFile2(k._2._1,k._2._2, k._2._3)
  case("InputFile3","OutputFile3","Table3") => SampleFile3(k._2._1,k._2._2, k._2._3)
  case _ => "Not Found"
})

    def SampleFile1(InputFile1: String, OutputFile1: String, Table1: String)= {
      import spark.implicits._

      val samplefile_pp_1_read = textFileRead(spark, InputFile1: String)
      val header = samplefile_pp_1_read.first()
      val file1FilterHeader = samplefile_pp_1_read.filter(row => row != header)

      val parse1 = file1FilterHeader.map(a => a.split(","))
      val mapping1 = parse1.map(x => Sample1File(x(0).toInt,x(1).toInt, x(2)))

      val xfrm1 = mapping1.withColumn("fix_bank_acct", format_string(s"%09d", $"bank_acct").cast("integer") ).withColumn("issue_dt", to_date($"issue_date", "yyyyMMdd"))
      val sample1filetestsc = xfrm1.printSchema()
      val sample1filetest = xfrm1.show(5,false)
      val xfrm2 = xfrm1.select($"bank_no",$"fix_bank_acct", $"issue_dt")


      val xfrm3 = xfrm2.select(concat_ws("", $"bank_no", $"fix_bank_acct",$"issue_dt"))

      textFileWrite(xfrm3, OutputFile1)
      Table_Write(spark, xfrm2, Table1)

    }

    def SampleFile2(InputFile2: String, OutputFile2: String, Table2: String)= {

      val samplefile_pp_2_read = textFileRead(spark, InputFile2: String)
      val header = samplefile_pp_2_read.first()
      val file2FilterHeader = samplefile_pp_2_read.filter(row => row != header)
      import spark.implicits._
      val parse1 = file2FilterHeader.map(a => a.split(","))
      val mapping1 = parse1.map(x => Sample2File(x(0).toInt, x(1).toInt, x(2), x(3).toDouble, x(4), x(5), x(6)))
      val sample2filetest = mapping1.show(5,false)
      val xfrm1 = mapping1.withColumn("fix_bank_acct", format_string("%010d", $"bank_acct").cast("integer"))
                          .withColumn("issue_dt", to_date($"issue_date", "yyyymmdd"))
                          .withColumn("void_ind_xfrm",  when(col("void_ind") === "V", "V").otherwise(" "))
                          .withColumn("SRC_NM", lit("DCS"))

      val xfrm2 = xfrm1.select($"bank_no",
        $"fix_bank_acct",
        $"issue_dt",
        $"check_amt",
        $"void_ind_xfrm",
        trim($"first_name").alias("first_name1"),
        trim($"last_name").alias("last_name1"),
      $"SRC_NM")


      val xfrm3 = xfrm2.select(concat_ws("", $"bank_no",$"fix_bank_acct", $"issue_dt", $"check_amt", $"void_ind_xfrm"))
      textFileWrite(xfrm3, OutputFile2)
      Table_Write(spark, xfrm2, Table2)
    }

    def SampleFile3(InputFile3: String, OutputFile3: String, Table3: String)= {
      import spark.implicits._

      val samplefile_pp_3_read = textFileRead(spark, InputFile3)
      val header = samplefile_pp_3_read.first()
      val file2FilterHeader = samplefile_pp_3_read.filter(row => row != header)
      val samplefile_pp_lkp = textFileRead(spark,"/user/govtham9/EDV_PP/samplefile_pp_lkp.txt")
      val samplefile_lkp_split = samplefile_pp_lkp.map{row => val x = row.split("\\|")
                                Map(x(0) -> x(1))}.collect().reduceLeft(_ ++ _)
      println("lookup data" + samplefile_lkp_split)
      val void_adden_map = (key: String) => samplefile_lkp_split.getOrElse(key, "not found")
      import org.apache.spark.sql.functions.udf
      val void_adden_lkp = udf(void_adden_map)

      import spark.implicits._
      val parse1 = file2FilterHeader.map(a => a.split(","))
     val mapping1 = parse1.map(x => Sample3File(x(0).toInt, x(1).toInt, x(2), x(3).toDouble, x(4), x(5), x(6), x(7)))

      val xfrm1 = mapping1
        .withColumn("fix_bank_acct", format_string("%010d", $"bank_acct").cast("Integer"))
          //.withColumn("fix_bank_acct", $"bank_acct")
        .withColumn("issue_dt", to_date($"issue_date", "yyyymmdd"))
        .withColumn("void_ind_xfrm",  when(col("void_ind") === "V", "V").otherwise(" "))
        .withColumn("SRC_NM", lit("DCS"))
      val sample3filetest = xfrm1.show(5,false)
      val xfrm2 = xfrm1.select($"bank_no", $"fix_bank_acct", $"issue_dt",$"check_amt", $"void_ind_xfrm", trim($"first_name").alias("first_name"), trim($"last_name").alias("last_name"), $"SRC_NM", void_adden_lkp($"void_adden"))
      val sample3tabletest = xfrm2.show(5,false)

      val xfrm3 = xfrm2.select(concat_ws("", $"bank_no", $"fix_bank_acct", $"issue_dt",$"check_amt", $"void_ind_xfrm", $"first_name", $"last_name"))
      val sample3ftest = xfrm3.show(5,false)
      //textFileWrite(xfrm3, OutputFile3)
      Table_Write(spark, xfrm2, Table3)

    }
  }

}
