package de.tuberlin.dima.bdapro.muses.connector

import java.sql.{DriverManager, ResultSet}
import java.{sql, util}

import org.apache.arrow.vector.complex.NonNullableStructVector
import org.apache.arrow.vector.complex.impl.{ComplexWriterImpl, SingleStructReaderImpl}
import org.apache.arrow.vector.complex.reader.BaseReader
import org.apache.arrow.vector.complex.writer._
import org.apache.arrow.vector.util.Text

//https://github.com/apache/arrow/blob/78152f113cf2a29b4c78b1c87d88a99fa4e92a29/java/vector/src/test/java/org/apache/arrow/vector/complex/writer/TestComplexWriter.java
//https://github.com/apache/arrow/blob/0894d97244951696ce880dfc3affdbae7a6c035c/java/vector/src/test/java/org/apache/arrow/vector/ipc/TestArrowFile.java

object Main2 {

  import org.apache.arrow.memory.RootAllocator

  private val allocator = new RootAllocator(Integer.MAX_VALUE)

  def main(args: Array[String]): Unit = {

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost/employees?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val username = "root"
    val password = "root"
    val query = "SELECT * FROM employees"

    var connection: sql.Connection = null

    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    var statement = connection.prepareStatement(query)
    val resultSet = statement.executeQuery
    val rsmd = resultSet.getMetaData
    val columnsNumber = rsmd.getColumnCount
    var i = 1
    println("Columns")
    var columns: util.ArrayList[Tuple2[String, String]] = new util.ArrayList[Tuple2[String, String]]
    for (i <- 1 to columnsNumber) {
      columns.add((rsmd.getColumnName(i), rsmd.getColumnTypeName(i)))
      System.out.println(rsmd.getColumnName(i))
    }

    def results[T](resultSet: ResultSet)(f: ResultSet => T) = {
      new Iterator[T] {
        def hasNext = resultSet.next()

        def next() = f(resultSet)
      }
    }

    println("---------------------")

    val it = results(statement.getResultSet) {
      case rs => rs
    }

    //...........................................................................

    println("-----------------------------------------------------------------")


    val parent = NonNullableStructVector.empty("parent", allocator)
    val writer = new ComplexWriterImpl("root", parent)
    val rootWriter = writer.rootAsStruct

    ////////////////////////////////////////
    var writers: util.ArrayList[(BaseWriter, String, String)] = new util.ArrayList[(BaseWriter, String, String)]()
    var readers: util.ArrayList[(BaseReader, String, String)] = new util.ArrayList[(BaseReader, String, String)]()

    writer.getClass
    columns.forEach(x => {
      if (x._2 == "INT") {
        var intWriter: IntWriter = rootWriter.integer(x._1)
        writers.add((intWriter, "IntWriter", x._1))
      } else if (x._2 == "DATE") {
        var dateMilliWriter: DateMilliWriter = rootWriter.dateMilli(x._1)
        writers.add((dateMilliWriter, "DateMilliWriter", x._1))
      } else if (x._2 == "VARCHAR" | x._2 == "CHAR") {
        var varCharWriter: VarCharWriter = rootWriter.varChar(x._1)
        writers.add((varCharWriter, "VarCharWriter", x._1))
      }
    })

    var count = -1
    while (it.hasNext) {
      var next = it.next()
      count += 1
      writers.forEach(x => {
        if (x._2.equalsIgnoreCase("IntWriter")) {
          var bigIntWriter: IntWriter = x._1.asInstanceOf[IntWriter]
          bigIntWriter.setPosition(count)
          bigIntWriter.writeInt(next.getObject(x._3).asInstanceOf[Integer])
        } else if (x._2.equalsIgnoreCase("DateMilliWriter")) {
          var bigIntWriter: DateMilliWriter = x._1.asInstanceOf[DateMilliWriter]
          bigIntWriter.setPosition(count)
          val date = next.getObject(x._3).asInstanceOf[java.util.Date]
          println("Date: " + date + "   :    " + date.getTime)
          bigIntWriter.writeDateMilli(date.getTime)
        } else if (x._2.equalsIgnoreCase("VarCharWriter")) {
          var varCharWriter: VarCharWriter = x._1.asInstanceOf[VarCharWriter]

          varCharWriter.setPosition(count)

          var strData = next.getObject(x._3).asInstanceOf[String]

          val bytes: Array[Byte] = strData.getBytes
          var tempBuf = allocator.buffer(bytes.length)
          tempBuf.setBytes(0, bytes)
          //          listWriter.startList()
          //          listWriter.varChar().writeVarChar(0, bytes.length, tempBuf)
          //          listWriter.endList()

          //          println("string data: " + strData)
          //
          //          val bytes: Array[Byte] = strData.getBytes
          //          var tempBuf = allocator.buffer(bytes.length)
          //
          //          tempBuf.setBytes(0, bytes)
          //
          varCharWriter.writeVarChar(0, bytes.length, tempBuf)
          //
        }
      })
      parent.setValueCount(count)
    }

    println("---------------------------------**************----------")
    val rootReader = new SingleStructReaderImpl(parent).reader("root")
    var i1 = 0
    while (i1 < parent.getValueCount) {
      rootReader.setPosition(i1)
      writers.forEach(x => {
        val reader = rootReader.reader(x._3)

        if (x._2.equalsIgnoreCase("IntWriter")) {
          val value = reader.readInteger()
          println(value)
        } else if (x._2.equalsIgnoreCase("DateMilliWriter")) {
          val value = reader.readLocalDateTime()
          println(value)
        } else if (x._2.equalsIgnoreCase("VarCharWriter")) {
          var value:Text = reader.readText()
          
          println(">>>>" + value.toString)
        }
      })
      i1 += 1
    }
    ////////////////////////////////////////
  }
}
