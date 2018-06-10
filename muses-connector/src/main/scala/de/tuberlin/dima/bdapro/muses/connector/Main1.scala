package de.tuberlin.dima.bdapro.muses.connector

import java.io.File
import java.sql.{Date, DriverManager, ResultSet}
import java.{sql, util}

import de.tuberlin.dima.bdapro.muses.connector.arrow.reader.ArrowReader
import org.apache.arrow.memory.{AllocationListener, BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.JsonFileWriter
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo._
import org.apache.arrow.vector.util.Text


object Main1 {

  def getArrowType(typeName: String) : ArrowType = {
    val arrowType:ArrowType = typeName match {
      case "INT"  => new ArrowType.Int(32, true)
      case "DATE"  => new ArrowType.Date(DateUnit.DAY)
      case "VARCHAR"  => new ArrowType.Utf8()
      case "CHAR"  => new ArrowType.Utf8()

      case _  => throw new Exception("Not supported.") // the default, catch-all
    }
    return arrowType
  }

  var arrowReader :ArrowReader = new ArrowReader
  var arrowWriter :ArrowReader = new ArrowReader
  def writeData(x: FieldVector, o: Object, count: Int ) : Unit = {

    var typ = x.getField.getType
    if (typ.isInstanceOf[ArrowType.Int]) {
      var value = o.asInstanceOf[Int]
      x.asInstanceOf[IntVector].setSafe(count, value)

      println(">>>>>>" , x.asInstanceOf[IntVector].getObject(count))
    }
    else if (typ.isInstanceOf[ArrowType.Date]) {
      var value = o.asInstanceOf[Date]
      x.asInstanceOf[DateDayVector].setSafe(count, value.toLocalDate.getYear)
      println(">>>>>>" , x.asInstanceOf[DateDayVector].getObject(count))
    } else if (typ.isInstanceOf[ArrowType.Utf8]) {
      var value = o.asInstanceOf[String]
      import java.nio.charset.Charset
      val utf8Charset = Charset.forName("UTF-8")
      val arrbuf = value.getBytes(utf8Charset)
      x.asInstanceOf[VarCharVector].setSafe(count, arrbuf)
      println(">>>>>>" , x.asInstanceOf[VarCharVector].getObject(count))
    } else {
      throw new Exception("No corresponding vector")
    }
  }

  def main(args: Array[String]): Unit = {
    // connect to the database named "mysql" on the localhost
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

      columns.add((rsmd.getColumnName(i),rsmd.getColumnTypeName(i)))

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
    //........................................................................................

    var fields:util.ArrayList[Field] = new util.ArrayList[Field]()
    columns.forEach(x => {
      fields.add(field(x._1, getArrowType(x._2)))
    })

    val sch = new Schema(fields)

    val allocator: BufferAllocator = new RootAllocator(AllocationListener.NOOP, Long.MaxValue)
    val schemaRoot = VectorSchemaRoot.create(sch, allocator)
    val fieldVectors = schemaRoot.getFieldVectors

    var count = -1
    while (it.hasNext) {
      var next = it.next()
      count += 1
      fieldVectors.forEach(x => {
        var name = x.getField.getName
        var o1 = next.getObject(name)
        writeData(x, o1, count)
        x.setValueCount(count)

      })
    }
    schemaRoot.setRowCount(count)
    //schemaRoot.close()
    /////////////////////////////////////
    //writing to json file just to see the schema
    
    val writer = new JsonFileWriter(new File("/home/mi/Desktop/empdata.json"), JsonFileWriter.config.pretty(true))
    writer.start(schemaRoot.getSchema, null)
    writer.write(schemaRoot)
    writer.close()
    ////////////////////////////////////
    val fieldVectorsReader = schemaRoot.getFieldVectors
var count11 = schemaRoot.getRowCount
    println("####################################: ")
    var ccc = 0
    for (ccc <- 0 until count11) {
      fieldVectorsReader.forEach(x => {
        var name = x.getField.getName
        var typ1 = x.getField.getType

        var dcount = x.getValueCount
        var ddd = 0
        //for (ddd <- 0 until dcount) {
          if (typ1.isInstanceOf[ArrowType.Int]) {
              var rec = x.asInstanceOf[IntVector].getObject(ccc)
              println(rec.asInstanceOf[Int])
            } else if (typ1.isInstanceOf[ArrowType.Date]) {
              var rec = x.asInstanceOf[DateDayVector].getObject(ccc)
              println(rec.asInstanceOf[Int])
            } else if (typ1.isInstanceOf[ArrowType.Utf8]) {
              var rec = x.asInstanceOf[VarCharVector].getObject(ccc)
              println(rec.asInstanceOf[Text])
            }
      })
    }

    println()
  }

  import scala.collection.JavaConverters._
  private def fieldx(name: String, nullable: Boolean, typ: ArrowType, children: Field*) = new Field(name, new FieldType(nullable, typ, null, null), children.toList.asJava)
  private def field(na: String, typ: ArrowType, chi: Field*) = fieldx(na, true, typ, chi: _*)
}
