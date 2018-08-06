package de.tuberlin.dima.bdapro.muses.connector.arrow.writer

import java.util

import org.apache.arrow.vector.complex.writer.{BaseWriter, DateMilliWriter, IntWriter, VarCharWriter}

object ArrowWriter {

  def writeDataWithWriters(columnName: String, dataType: String, rootWriter: BaseWriter.StructWriter): (BaseWriter, String, String) = {
    var vector: (BaseWriter, String, String)= null
    //TODO: Implement for more data types
    if (dataType == "INT") {
      var intWriter: IntWriter = rootWriter.integer(columnName)
      vector = new Tuple3[BaseWriter, String, String](intWriter, "IntWriter", columnName)
    } else if (dataType == "DATE") {
      var dateMilliWriter: DateMilliWriter = rootWriter.dateMilli(columnName)
      vector= new Tuple3[BaseWriter, String, String](dateMilliWriter, "DateMilliWriter", columnName)
    } else if (dataType == "VARCHAR" | dataType == "CHAR") {
      var varCharWriter: VarCharWriter = rootWriter.varChar(columnName)
      vector = new Tuple3[BaseWriter, String, String](varCharWriter, "VarCharWriter", columnName)
    }
    return vector
  }
}
