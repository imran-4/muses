package de.tuberlin.dima.bdapro.muses.connector

import java.sql.{DriverManager, ResultSet}
import java.{sql, util}

import org.apache.arrow.memory.{AllocationListener, BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo._


object Main1 {
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
    for (i <- 1 to columnsNumber) {
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

    var field1: Field = field("emp_no", new ArrowType.Int(32, true))
    var field2: Field = field("birth_date", new ArrowType.Date(DateUnit.DAY))
    var field3: Field = field("first_name", new ArrowType.Utf8())
    var field4: Field = field("last_name", new ArrowType.Utf8())
    var field5: Field = field("gender", new ArrowType.Utf8())
    var field6: Field = field("hire_date", new ArrowType.Date(DateUnit.DAY))

    var fieldList = new util.ArrayList[Field]()
    fieldList.add(field1)
    fieldList.add(field2)
    fieldList.add(field3)
    fieldList.add(field4)
    fieldList.add(field5)
    fieldList.add(field6)

    val sch = new Schema(fieldList)

    val allocator: BufferAllocator = new RootAllocator(AllocationListener.NOOP, Long.MaxValue)

    val schemaRoot = VectorSchemaRoot.create(sch, allocator)

    var fieldVector1 = schemaRoot.getVector("emp_no").getDataBuffer

    schemaRoot.getFieldVectors.get(0).allocateNew
    val vector = schemaRoot.getFieldVectors.get(0).asInstanceOf[IntVector]

    val fieldVectors = schemaRoot.getFieldVectors
    var empnoVector = fieldVectors.get(0).asInstanceOf[IntVector]
    println("--------------------")

    var vex = schemaRoot.getVector("first_name")
    var valueCount = 0
    while(it.hasNext) {
      var data = it.next().getInt(1)

      empnoVector.setSafe(valueCount, data)

      valueCount += 1
    }

    empnoVector.setValueCount(valueCount)

    val firstVector = schemaRoot.getFieldVectors.get(0)

    var newcount = firstVector.getValueCount
    var vc = 0
    for (vc <- 0 until newcount) {

     var it1 = firstVector.getObject(vc)

      println(it1)
    }
  }

  import scala.collection.JavaConverters._

  private def fieldx(name: String, nullable: Boolean, typ: ArrowType, children: Field*) = new Field(name, new FieldType(nullable, typ, null, null), children.toList.asJava)

  private def field(na: String, typ: ArrowType, chi: Field*) = fieldx(na, true, typ, chi: _*)

}
