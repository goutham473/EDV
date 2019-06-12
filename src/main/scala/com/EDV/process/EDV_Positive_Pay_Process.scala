package com.EDV.process

import com.EDV.file_read.EDV_File_Read
import com.EDV.file_write.EDV_File_Write
import com.EDV.table_write.EDV_Table_Write
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

case class File1(bank_no: Int, bank_acct: Int, issue_date: String, check_amt: Double, void_ind: String, first_name: String, last_name: String)

object EDV_Positive_Pay_Process extends EDV_File_Read with EDV_File_Write with EDV_Table_Write {
def positive_pay(spark: SparkSession, InputPath: String, OutputPath: String, Table: String) {
  import spark.implicits._

  val FileRead1 = textFileRead(spark, InputPath: String)
  val header = FileRead1.first()
  val file1FilterHeader = FileRead1.filter(row => row != header)

  val parse1 = file1FilterHeader.map(x => x.split(",")) //DataSet[Arry[String]] ==> Enoder[Array[String]] => binary format
  val mapping1 = parse1.map(x => File1(x(0).toInt, x(1).toInt, x(2), x(3).toDouble, x(4), x(5), x(6)))

  val xfrm_bnk_acct = mapping1.withColumn("fix_bank_acct", format_string("%10d", $"bank_acct")).withColumn("isse_date_tble", to_date($"issue_date", "yyyyMMdd")).withColumn("void_ind_xfrm", when(col("void_ind") === "V", "V").otherwise(" ")).withColumn("SRC_NM", lit("DCS"))
  //val xfrm_trim = xfrm_bnk_acct.select($"bank_no", $"bank_acct", $"isse_date_tble", $"check_amt", $"void_ind", trim($"first_name").alias("first_name"), trim($"last_name").alias("last_name"), $"fix_bank_acct", $"isse_date_tble", $"void_ind_xfrm", $"SRC_NM")
  val xfrm_trim = xfrm_bnk_acct.select($"bank_no", $"bank_acct", $"isse_date_tble", $"check_amt", $"void_ind", trim($"first_name").alias("first_name"), trim($"last_name").alias("last_name"), $"SRC_NM")
  //xfrm_trim.createOrReplaceTempView("samplefile")
  //spark.sql(s"insert into table dpl.samplefile_txt select bank_no,bank_acct,isse_date_tble,check_amt,void_ind,first_name,last_name,SRC_NM from samplefile")
  val fileWrite1 = xfrm_trim.select(concat_ws("", $"bank_no", $"bank_acct", regexp_replace($"isse_date_tble", "-", ""), $"check_amt", $"void_ind", $"first_name", $"last_name"))
  textFileWrite(fileWrite1,OutputPath)
  Table_Write(spark, xfrm_trim, Table)


}

}
