package de.tuberlin.dima.bdapro.muses.connector

import java.io._
import java.nio.charset.StandardCharsets
import java.sql.ResultSet
import java.util

import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowReader}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.joda.time.LocalDate

//import de.tuberlin.dima.bdapro.muses.connector.Main1.{field, getArrowType, writeData}
import de.tuberlin.dima.bdapro.muses.connector.rdbms.reader.DataReader
import org.apache.arrow.memory.{AllocationListener, BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo._


class Test {
  def getArrowType(typeName: String) : ArrowType = {
    val arrowType:ArrowType = typeName match {
      case "INT"  => new ArrowType.Int(32, true)
      case "DATE"  => new ArrowType.Date(DateUnit.DAY)
      case "VARCHAR" | "CHAR"  => new ArrowType.Utf8

      case _  => throw new Exception("Not supported.") // the default, catch-all
    }
    return arrowType
  }
  def writeData(x: FieldVector, o: Object, count: Int, allocator: BufferAllocator ) : Unit = {
    var typ = x.getField.getType
    if (typ.isInstanceOf[ArrowType.Int]) {
      var value = o.asInstanceOf[Int]
      x.asInstanceOf[IntVector].setSafe(count, value)
      x.setValueCount(count)
    } else if (typ.isInstanceOf[ArrowType.Date]) {
      var value = LocalDate.fromDateFields(o.asInstanceOf[java.util.Date])
      import org.joda.time.format.DateTimeFormat
      val formatter = DateTimeFormat.forPattern("yyyyMMdd")
      val lvalue = value.toString(formatter)
      x.asInstanceOf[DateDayVector].setSafe(count, Integer.parseInt(lvalue).intValue())//count, DateUtility.daysToStandardMillis(value))111
      x.setValueCount(count)
    } else if (typ.isInstanceOf[ArrowType.Utf8]) {
      var value = o.asInstanceOf[String]
      x.asInstanceOf[VarCharVector].setSafe(count, value.getBytes, 0, value.getBytes.length)
//      x.asInstanceOf[VarCharVector].setValueLengthSafe(count, value.getBytes(StandardCharsets.UTF_8).length)
    } else {
      throw new Exception("No corresponding vector")
    }
  }

  def readDatabase(): (ResultSet, util.ArrayList[(String, String)]) = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost/employees?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val username = "root"
    val password = "root"
    val query = "SELECT * FROM employees"

    val reader = new DataReader()
    reader.loadDrivers(driver)
    val connection = reader.getConnection(url, username, password)
    val statement =  reader.getPreparedStatement(connection, query)
    val resultSet = reader.getResultSet(statement)
    val columns = reader.getColumnNames(reader.getMetaData(resultSet))
    return (resultSet, columns)
  }
  import scala.collection.JavaConverters._
  private def fieldx(name: String, nullable: Boolean, typ: ArrowType, children: Field*) = new Field(name, new FieldType(nullable, typ, null, null), children.toList.asJava)
  private def field(na: String, typ: ArrowType, chi: Field*) = fieldx(na, true, typ, chi: _*)

  def write(): (VectorSchemaRoot, RootAllocator) = {
    val (it, columns) = readDatabase()
    import com.google.common.collect.ImmutableList
    var parentBuilder:ImmutableList.Builder[Field] = ImmutableList.builder()
    columns.forEach(x => {
      var f = field(x._1, getArrowType(x._2))
      parentBuilder.add(f)
    })

    var allocator = new RootAllocator(AllocationListener.NOOP, Long.MaxValue)
    import java.util

    import org.apache.arrow.vector.FieldVector
    val fieldVectors = new util.ArrayList[FieldVector]
    var sch:Schema = new Schema(parentBuilder.build)

    import scala.collection.JavaConversions._
    for (field <- sch.getFields) {
//      if (field.getFieldType.getType.isInstanceOf[ArrowType.Utf8]) {
//        val vector = new VarCharVector(field.getName, new FieldType(true, new ArrowType.Utf8, null), allocator)
//        vector.allocateNew()
//        fieldVectors.add(vector)
//
//      } else {
      val vector = field.createVector(allocator)
      vector.allocateNew()
      fieldVectors.add(vector)
//      }
    }
    println("SCHEMA>>>>>>>>>>>>>>"  + sch.toJson)
    var count = -1
    while (it.next()) {
//      var next = it.get
      count += 1
      fieldVectors.forEach(x => {
        var name = x.getField.getName
        var o1 = it.getObject(name)
        writeData(x, o1, count, allocator)

      })
    }
//    schemaRoot.setRowCount(count)
    //schemaRoot.close()



    val schemaRoot: VectorSchemaRoot = new VectorSchemaRoot(sch.getFields(), fieldVectors, count)
    return (schemaRoot, allocator)
  }

  def read(schemaRoot: VectorSchemaRoot, allocator: RootAllocator): FieldVector = {
    val fieldVectorsReader = schemaRoot.getFieldVectors
    var count11 = schemaRoot.getRowCount
    var ccc = 0
    val file = new File("/home/mi/Desktop/out.csv")
    val bw = new BufferedWriter(new FileWriter(file))

    var fieldsString:String = ""
    schemaRoot.getSchema.getFields.forEach(x => fieldsString += x.getName + ",")
    fieldsString += "\r\n"
    bw.write(fieldsString)
    for (ccc <- 0 to count11) {
      var str:StringBuilder = new StringBuilder
      fieldVectorsReader.forEach(x => {
        var name = x.getField.getName
        var typ1 = x.getField.getType
        var dcount = x.getValueCount

        if (typ1.isInstanceOf[ArrowType.Int]) {
          var rec = x.asInstanceOf[IntVector].getObject(ccc)
          println("$$$$$ " + rec)
          str.append(rec + ",")
        } else if (typ1.isInstanceOf[ArrowType.Date]) {
          var rec = x.asInstanceOf[DateDayVector].getObject(ccc)
          println("$$$$$+++ " + rec)
          str.append(rec+ ",")
        } else if (typ1.isInstanceOf[ArrowType.Utf8]) {

          println(">>>>>>>>>>>>" + ccc)
          var rec = x.asInstanceOf[VarCharVector].getObject(ccc)
          println(rec)
          str.append(rec+ ",")
        }
      })
      str.append("\r\n")
      println("=====================++++++++++++++++==============")
      println(str.toString())
      bw.write(str.toString())
    }

//    fieldVectorsReader.get(0).
//    ArrowRecordBatch arr = new ArrowRecordBatch()
    return fieldVectorsReader.get(0)//.iterator().asInstanceOf[Iterator[Object]]
//    return fieldVectorsReader.get(0).iterator()
    //    Source.fromIterable(new Iterable () {
    //      fieldVectorsReader.get(0).iterator()
    //    })

    //    fieldVectorsReader.get(0).iterator()
//    bw.close()
  }

  def execute(): FieldVector = {
    var (vec, allocator) = write()
    return read(vec, allocator)
  }
}

object Main1 {
  def main(args: Array[String]): Unit = {
    val test = new Test()
//    test.read(test.write())
    var (vec, allocator) = test.write()
    test.read(vec, allocator)
  }
}

//...........................................................................
//object Main1 {
//  def getArrowType(typeName: String) : ArrowType = {
//    val arrowType:ArrowType = typeName match {
//      case "INT"  => new ArrowType.Int(32, true)
//      case "DATE"  => new ArrowType.Date(DateUnit.DAY)
//      case "VARCHAR"  => new ArrowType.Utf8()
//      case "CHAR"  => new ArrowType.Utf8()
//
//      case _  => throw new Exception("Not supported.") // the default, catch-all
//    }
//
//    return arrowType
//  }
//
//  var arrowReader :ArrowReader = new ArrowReader
//  var arrowWriter :ArrowReader = new ArrowReader
//  def writeData(x: FieldVector, o: Object, count: Int, allocator: BufferAllocator ) : Unit = {
//    var typ = x.getField.getType
//    if (typ.isInstanceOf[ArrowType.Int]) {
//      var value = o.asInstanceOf[Int]
//      x.asInstanceOf[IntVector].set(count, value)
//    }
//    else if (typ.isInstanceOf[ArrowType.Date]) {
//      var value = o.asInstanceOf[Date]
//      x.asInstanceOf[DateDayVector].setSafe(count, value.toLocalDate.getYear)
//    } else if (typ.isInstanceOf[ArrowType.Utf8]) {
//      var value = o.asInstanceOf[String]
////      import java.nio.charset.Charset
////      val utf8Charset:Charset = Charset.forName("UTF-8")
//      val arrbuf = value.getBytes(StandardCharsets.UTF_8)
//      x.asInstanceOf[VarCharVector].setSafe(count, arrbuf, 0, arrbuf.length)
//      println(">>>>>>########" , x.asInstanceOf[VarCharVector].getObject(count))
//    } else {
//      throw new Exception("No corresponding vector")
//    }
//  }
//
//  def main(args: Array[String]): Unit = {
//    // connect to the database named "mysql" on the localhost
//    val driver = "com.mysql.jdbc.Driver"
//    val url = "jdbc:mysql://localhost/employees?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
//    val username = "root"
//    val password = "root"
//    val query = "SELECT * FROM employees"
//
//    var connection: sql.Connection = null
//
//    Class.forName(driver)
//    connection = DriverManager.getConnection(url, username, password)
//    var statement = connection.prepareStatement(query)
//    val resultSet = statement.executeQuery
//    val rsmd = resultSet.getMetaData
//    val columnsNumber = rsmd.getColumnCount
//    var i = 1
//    println("Columns")
//    var columns: util.ArrayList[Tuple2[String, String]] = new util.ArrayList[Tuple2[String, String]]
//    for (i <- 1 to columnsNumber) {
//      columns.add((rsmd.getColumnName(i),rsmd.getColumnTypeName(i)))
//      System.out.println(rsmd.getColumnName(i))
//    }
//
//    def results[T](resultSet: ResultSet)(f: ResultSet => T) = {
//      new Iterator[T] {
//        def hasNext = resultSet.next()
//        def next() = f(resultSet)
//      }
//    }
//
//    println("---------------------")
//
//    val it = results(statement.getResultSet) {
//      case rs => rs
//    }
//    //........................................................................................
//
//    columns.forEach(x => println(x))
//    var fields:util.ArrayList[Field] = new util.ArrayList[Field]()
//    columns.forEach(x => {
//      fields.add(field(x._1, getArrowType(x._2)))
//    })
//    val sch = new Schema(fields)
//    println(sch.toString)
//    val allocator: BufferAllocator = new RootAllocator(AllocationListener.NOOP, Long.MaxValue)
//    val schemaRoot = VectorSchemaRoot.create(sch, allocator)
//    val fieldVectors = schemaRoot.getFieldVectors
//
//    fieldVectors.forEach(x => {
//      if (x.isInstanceOf[ArrowType.Utf8]) {
//        x.getDataBuffer.alloc()
//
////        println(x.allocateNewSafe())
//      }
//    })
//
//    println("***********************")
//    var count = -1
//    while (it.hasNext) {
//      var next = it.next()
//      count += 1
//      fieldVectors.forEach(x => {
//        var name = x.getField.getName
//        var o1 = next.getObject(name)
//        writeData(x, o1, count, allocator)
//        x.setValueCount(count)
//
//      })
//    }
//    schemaRoot.setRowCount(count)
//    println("***********************")
//    //schemaRoot.close()
//    /////////////////////////////////////
//    //writing to json file just to see the schema
//
//    val writer = new JsonFileWriter(new File("/home/mi/Desktop/empdata.json"), JsonFileWriter.config.pretty(true))
//    writer.start(schemaRoot.getSchema, null)
//    writer.write(schemaRoot)
//    writer.close()
//    ////////////////////////////////////
//    val fieldVectorsReader = schemaRoot.getFieldVectors
//    var count11 = schemaRoot.getRowCount
//    println("####################################")
//    var ccc = 0
//
//    val file = new File("/home/mi/Desktop/out.csv")
//    val bw = new BufferedWriter(new FileWriter(file))
//
//    var fieldsString:String = ""
//    schemaRoot.getSchema.getFields.forEach(x => fieldsString += x.getName + ",")
//    fieldsString += "\r\n"
//    bw.write(fieldsString)
//
//    for (ccc <- 0 until count11) {
//      var str:StringBuilder = new StringBuilder
//      fieldVectorsReader.forEach(x => {
//        var name = x.getField.getName
//        var typ1 = x.getField.getType
//        var dcount = x.getValueCount
//        if (typ1.isInstanceOf[ArrowType.Int]) {
//          var rec = x.asInstanceOf[IntVector].getObject(ccc)
//          str.append(rec + ",")
//        } else if (typ1.isInstanceOf[ArrowType.Date]) {
//          var rec = x.asInstanceOf[DateDayVector].getObject(ccc)
//          str.append(rec+ ",")
//        } else if (typ1.isInstanceOf[ArrowType.Utf8]) {
//          println("----------========" + ccc)
//          var rec = x.asInstanceOf[VarCharVector].getObject(0)
//          println(rec)
//          str.append(rec+ ",")
//        }
//      })
//      str.append("\r\n")
//      bw.write(str.toString())
//    }
//
//
////    Source.fromIterable(new Iterable () {
////      fieldVectorsReader.get(0).iterator()
////    })
//
////    fieldVectorsReader.get(0).iterator()
//    bw.close()
//
//    println("")
//  }
//
//  import scala.collection.JavaConverters._
//  private def fieldx(name: String, nullable: Boolean, typ: ArrowType, children: Field*) = new Field(name, new FieldType(nullable, typ, null, null), children.toList.asJava)
//  private def field(na: String, typ: ArrowType, chi: Field*) = fieldx(na, true, typ, chi: _*)
//}