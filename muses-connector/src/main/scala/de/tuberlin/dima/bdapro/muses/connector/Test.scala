package de.tuberlin.dima.bdapro.muses.connector

import java.io._
import java.sql.ResultSet
import java.util

import org.joda.time.LocalDate
import de.tuberlin.dima.bdapro.muses.connector.rdbms.reader.DataReader
import org.apache.arrow.memory.{AllocationListener, BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
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

  def read(schemaRoot: VectorSchemaRoot, allocator: RootAllocator): VectorSchemaRoot = {
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
    return schemaRoot //.iterator().asInstanceOf[Iterator[Object]]
//    return fieldVectorsReader.get(0).iterator()
    //    Source.fromIterable(new Iterable () {
    //      fieldVectorsReader.get(0).iterator()
    //    })

    //    fieldVectorsReader.get(0).iterator()
//    bw.close()
  }

  def execute(): ArrowRecordBatch = {
//    var (vec, allocator) = write()
//    return read(vec, allocator)


    var (vec, allocator) = write()
    var schemaRoot= read(vec, allocator)

    var schema = schemaRoot.getSchema
    val vectorUnloader = new VectorUnloader(schemaRoot)

    val recordBatch = vectorUnloader.getRecordBatch

    println("RECORD BARCH LENGTH: " + recordBatch.getLength)

    print("++++++++++++++++  " + recordBatch.getNodes.get(0).toString)

    return recordBatch
//    try {
//      val recordBatch = vectorUnloader.getRecordBatch
//
//      println("RECORD BARCH LENGTH: " + recordBatch.getLength)
//
//      val finalVectorsAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE)
//      val newRoot = VectorSchemaRoot.create(schema, finalVectorsAllocator)
//      try { // load it
//        val vectorLoader = new VectorLoader(newRoot)
//        vectorLoader.load(recordBatch)
//        val intReader = newRoot.getVector("emp_no").getReader
//        //        val bigIntReader = newRoot.getVector("bigInt").getReader
//        var i = 0
//        var count = 20
//        while (i < count) {
//          intReader.setPosition(i)
//          println(intReader.readInteger().intValue())
//          i += 1
//        }
//      } finally {
//        if (recordBatch != null) recordBatch.close()
//        if (finalVectorsAllocator != null) finalVectorsAllocator.close()
//        if (newRoot != null) newRoot.close()
//      }
//    }



  }
}

object Main1 {
  def main(args: Array[String]): Unit = {
    val test = new Test()
//    test.read(test.write())
    var (vec, allocator) = test.write()
    var schemaRoot= test.read(vec, allocator)


    //for main2:  https://github.com/apache/arrow/blob/78152f113cf2a29b4c78b1c87d88a99fa4e92a29/java/vector/src/test/java/org/apache/arrow/vector/TestVectorUnloadLoad.java


//    import org.apache.arrow.vector.FieldVector
//    import org.apache.arrow.vector.VectorLoader
//    import org.apache.arrow.vector.VectorSchemaRoot
//    import org.apache.arrow.vector.VectorUnloader
//    import org.apache.arrow.vector.complex.reader.FieldReader
//    import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
    //val root = parent.getChild("root")

//    var schema = schemaRoot.getSchema
//    val vectorUnloader = new VectorUnloader(schemaRoot)
//    try {
//      val recordBatch = vectorUnloader.getRecordBatch
//
//      println("RECORD BARCH LENGTH: " + recordBatch.getLength)
//
//      val finalVectorsAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE)
//      val newRoot = VectorSchemaRoot.create(schema, finalVectorsAllocator)
//      try { // load it
//        val vectorLoader = new VectorLoader(newRoot)
//        vectorLoader.load(recordBatch)
//        val intReader = newRoot.getVector("emp_no").getReader
////        val bigIntReader = newRoot.getVector("bigInt").getReader
//        var i = 0
//        var count = 20
//        while (i < count) {
//          intReader.setPosition(i)
//          println(intReader.readInteger().intValue())
//            i += 1
//        }
//      } finally {
//        if (recordBatch != null) recordBatch.close()
//        if (finalVectorsAllocator != null) finalVectorsAllocator.close()
//        if (newRoot != null) newRoot.close()
//      }
//    }



    var schema = schemaRoot.getSchema
    val vectorUnloader = new VectorUnloader(schemaRoot)

    val recordBatch = vectorUnloader.getRecordBatch

    println("RECORD BARCH LENGTH: " + recordBatch.getLength)

    print("++++++++++++++++  " + recordBatch.getNodes.get(0).toString)




  }
}