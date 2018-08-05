package de.tuberlin.dima.bdapro.muses.connector

import java.sql.ResultSet
import java.util

import de.tuberlin.dima.bdapro.muses.connector.arrow.writer.ArrowWriter
import de.tuberlin.dima.bdapro.muses.connector.rdbms.connectionmanager.JDBCDriversInfo
import de.tuberlin.dima.bdapro.muses.connector.rdbms.reader.DataReader
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.NonNullableStructVector
import org.apache.arrow.vector.complex.impl.{ComplexWriterImpl, SingleStructReaderImpl}
import org.apache.arrow.vector.complex.writer._
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{DateMilliVector, FieldVector, IntVector, VarCharVector}

//https://github.com/apache/arrow/blob/78152f113cf2a29b4c78b1c87d88a99fa4e92a29/java/vector/src/test/java/org/apache/arrow/vector/complex/writer/TestComplexWriter.java
//https://github.com/apache/arrow/blob/0894d97244951696ce880dfc3affdbae7a6c035c/java/vector/src/test/java/org/apache/arrow/vector/ipc/TestArrowFile.java
//https://github.com/apache/arrow/blob/78152f113cf2a29b4c78b1c87d88a99fa4e92a29/java/vector/src/test/java/org/apache/arrow/vector/TestVectorUnloadLoad.java

//TODO: REFACTOR THE CODE

object Main2 {
  private val allocator = new RootAllocator(Integer.MAX_VALUE)
  val parent: NonNullableStructVector = NonNullableStructVector.empty("parent", allocator)
  val writer = new ComplexWriterImpl("root", parent)

  def writeToArrow(resultSet: ResultSet, columns: util.List[(String, String)]): NonNullableStructVector = {
    val rootWriter = writer.rootAsStruct
    var vectors = new util.ArrayList[(BaseWriter, String, String)]()

    columns.forEach(x => {
      vectors.add(ArrowWriter.writeDataWithWriters(x._1, x._2, rootWriter))
    })

    var count = -1
    while (resultSet.next()) {
      count += 1
      vectors.forEach(x => {
        var typeOfWriter = x._2
        if (x._2.equalsIgnoreCase("IntWriter")) {
          var bigIntWriter: IntWriter = x._1.asInstanceOf[IntWriter]
          bigIntWriter.setPosition(count)
          bigIntWriter.writeInt(resultSet.getObject(x._3).asInstanceOf[Integer])
          parent.setValueCount(count)
        } else if (x._2.equalsIgnoreCase("DateMilliWriter")) {
          var bigIntWriter: DateMilliWriter = x._1.asInstanceOf[DateMilliWriter]
          bigIntWriter.setPosition(count)
          val date = resultSet.getObject(x._3).asInstanceOf[java.util.Date]
          bigIntWriter.writeDateMilli(date.getTime)
          parent.setValueCount(count)
        } else if (x._2.equalsIgnoreCase("VarCharWriter")) {
          var varCharWriter: VarCharWriter = x._1.asInstanceOf[VarCharWriter]
          varCharWriter.setPosition(count)
          var strData = resultSet.getObject(x._3).asInstanceOf[String]
          val bytes: Array[Byte] = strData.getBytes
          var tempBuf = allocator.buffer(bytes.length)
          tempBuf.setBytes(0, bytes)
          varCharWriter.writeVarChar(0, bytes.length, tempBuf)
        }
      })
    }
    return (parent)
  }

  def readFromArrow(parent: NonNullableStructVector): Unit = {
    val rootReader = new SingleStructReaderImpl(parent).reader("root")
    var root: FieldVector = parent.getChild("root")
    var schema: Schema = new Schema(root.getField.getChildren())
    var vectors = root.getChildrenFromFields
    var i1 = 0
    while (i1 < parent.getValueCount) {
      println("***************************************")
      rootReader.setPosition(i1)
      vectors.forEach(x => {
        var reader = rootReader.reader(x.getField.getName)
        if (x.isInstanceOf[IntVector]) {
          val value = reader.readInteger()
          println(value)
        } else if (x.isInstanceOf[DateMilliVector]) {
          val value = reader.readLocalDateTime()
          println(value)
        } else if (x.isInstanceOf[VarCharVector]) {
          var value = reader.readText()
          println(value)
        }
      })
      i1 += 1
    }


    //get ArrowRecordBatch here
  }

  def readDatabase(): (util.ArrayList[(String, String)], ResultSet) = {
    val driver = JDBCDriversInfo.MYSQL_DRIVER
    val url = "jdbc:mysql://localhost/employees?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val username = "root"
    val password = "root"
    val query = "SELECT * FROM employees"
    val reader = new DataReader()
    reader.loadDrivers(driver)
    val connection = reader.getConnection(url, username, password)
    val statement = reader.getPreparedStatement(connection, query)
    val resultSet = reader.getResultSet(statement)
    val columns = reader.getColumnNames(reader.getMetaData(resultSet))
    return (columns, resultSet)
  }

  def execute(): Unit = {
    var (columns, resultSet) = readDatabase
    println("-----------------------------------------------------------------")
    var parent = writeToArrow(resultSet, columns)
    readFromArrow(parent)
  }

  def main(args: Array[String]): Unit = {
    var (columns, resultSet) = readDatabase
    println("-----------------------------------------------------------------")
    var parent = writeToArrow(resultSet, columns)
    readFromArrow(parent)
  }
}