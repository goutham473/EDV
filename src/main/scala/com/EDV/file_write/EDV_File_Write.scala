package com.EDV.file_write

import org.apache.spark.sql.DataFrame

trait EDV_File_Write {
  def textFileWrite(DF: DataFrame, OutputPath: String): Unit = DF.write.mode("overwrite").text(OutputPath)
}
