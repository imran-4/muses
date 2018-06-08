package de.tuberlin.dima.bdapro.muses.connector

import java.sql.{DriverManager, ResultSet}
import java.{sql, util}

import org.apache.arrow.memory.{AllocationListener, BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo._


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

  def getFieldVector(typ: ArrowType, x: FieldVector) : FieldVector = {
    var vectorType: FieldVector = null
    if (typ.isInstanceOf[ArrowType.Int]) {
      vectorType = x.asInstanceOf[IntVector]
    } else if (typ.isInstanceOf[ArrowType.Date]) {
      vectorType = x.asInstanceOf[DateDayVector]
    } else if (typ.isInstanceOf[ArrowType.Utf8]) {
      //vectorType = x.asInstanceOf[VarCharVector]
    } else {
      throw new Exception("No corresponding vector")
    }
    return vectorType

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

//

    val sch = new Schema(fields)

    val allocator: BufferAllocator = new RootAllocator(AllocationListener.NOOP, Long.MaxValue)
    val schemaRoot = VectorSchemaRoot.create(sch, allocator)
    val fieldVectors = schemaRoot.getFieldVectors

//
//    var vectors:util.ArrayList[FieldVector] = new util.ArrayList[FieldVector]
//    fieldVectors.forEach(x=> {
//      var typ = x.getField.getFieldType.getType
//      vectors.add(getFieldVector(typ, x))
//      vectors.get(0).setSafe()
//
//    })
//    println()
//
    var count = 0
    while (it.hasNext) {
      fieldVectors.forEach(x => {
        var name = x.getField.getName
        var typ1 = x.getField.getType
        var next = it.next()
        if (typ1.isInstanceOf[ArrowType.Int]){
          var id = next.getInt(name)

          x.asInstanceOf[IntVector].setSafe(count, id) //
          println()
        } else if (typ1.isInstanceOf[ArrowType.Date]) {

        }


      })
      count += 1
    }

println()


    //    fieldVectors.forEach(x=> {
//      var typ = x.getField.getFieldType.getType
//      val vector = getFieldVector(typ, x)
//      while (it.hasNext) {
//        var i : Int = 0
//        for (i <- 0 until columns.size()) {
//          var data = it.next().getObject(i)
//          vector.setSafe(, data)
//        }
//        println(data)
//      }
//    })



//
//    val temp:IntVector = fieldVectors.get(0).asInstanceOf[IntVector]
//    var empnoVector = fieldVectors.get(0).asInstanceOf[IntVector]
//    println("--------------------")
//
//    var vex = schemaRoot.getVector("first_name")
//    var valueCount = 0
//    while(it.hasNext) {
//
//      var data = it.next().getObject(0)
//
//      empnoVector.setSafe(valueCount, data)
//
//      valueCount += 1
//    }
//
//    empnoVector.setValueCount(valueCount)
//
//    val firstVector = schemaRoot.getFieldVectors.get(0)
//
//    var newcount = firstVector.getValueCount
//    var vc = 0
//    for (vc <- 0 until newcount) {
//
//     var it1 = firstVector.getObject(vc)
//
//      println(it1)
//    }
  }

  import scala.collection.JavaConverters._

  private def fieldx(name: String, nullable: Boolean, typ: ArrowType, children: Field*) = new Field(name, new FieldType(nullable, typ, null, null), children.toList.asJava)

  private def field(na: String, typ: ArrowType, chi: Field*) = fieldx(na, true, typ, chi: _*)

}
