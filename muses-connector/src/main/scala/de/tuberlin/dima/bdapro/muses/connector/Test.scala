package de.tuberlin.dima.bdapro.muses.connector

import java.io._
import java.nio.channels.Channels
import java.sql.ResultSet
import java.util

import com.google.common.collect.ImmutableList
import de.tuberlin.dima.bdapro.muses.connector.arrow.writer.{ArrowVectorsWriter, ArrowWriter}
import de.tuberlin.dima.bdapro.muses.connector.rdbms.connectionmanager.JDBCDriversInfo
import de.tuberlin.dima.bdapro.muses.connector.rdbms.reader.DataReader
import org.apache.arrow.memory.{AllocationListener, BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.{ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, MessageSerializer}
import org.apache.arrow.vector.types.pojo._
import org.apache.arrow.vector.{FieldVector, _}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class Test {
  def readDatabase(): (ResultSet, util.ArrayList[(String, String)]) = {
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
    return (resultSet, columns)
  }

  private def fieldx(name: String, nullable: Boolean, typ: ArrowType, children: Field*) = new Field(name, new FieldType(nullable, typ, null, null), children.toList.asJava)

  private def field(na: String, typ: ArrowType, chi: Field*) = fieldx(na, true, typ, chi: _*)

  def write(): (VectorSchemaRoot, RootAllocator) = {
    val (it, columns) = readDatabase()
    var parentBuilder: ImmutableList.Builder[Field] = ImmutableList.builder()
    columns.forEach(x => {
      var f = field(x._1, POJOArrowTypeMappings.getArrowType(x._2))
      parentBuilder.add(f)
    })

    var allocator = new RootAllocator(AllocationListener.NOOP, Long.MaxValue)
    val fieldVectors = new util.ArrayList[FieldVector]
    var sch: Schema = new Schema(parentBuilder.build)

    for (field <- sch.getFields) {
      val vector = field.createVector(allocator)
      vector.allocateNew()
      fieldVectors.add(vector)
    }
    println("SCHEMA>>>>>>>>>>>>>>" + sch.toJson)
    var count = -1
    while (it.next()) {
      count += 1
      fieldVectors.forEach(x => {
        var name = x.getField.getName
        var o1 = it.getObject(name)
        ArrowVectorsWriter.writeFieldVector(x, o1, count)
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

    var fieldsString: String = ""
    schemaRoot.getSchema.getFields.forEach(x => fieldsString += x.getName + ",")
    fieldsString += "\r\n"
    bw.write(fieldsString)
    for (ccc <- 0 to count11) {
      var str: StringBuilder = new StringBuilder
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
          str.append(rec + ",")
        } else if (typ1.isInstanceOf[ArrowType.Utf8]) {

          println(">>>>>>>>>>>>" + ccc)
          var rec = x.asInstanceOf[VarCharVector].getObject(ccc)
          println(rec)
          str.append(rec + ",")
        }
      })
      str.append("\r\n")
      println("=====================++++++++++++++++==============")
      println(str.toString())
      bw.write(str.toString())
    }
    return schemaRoot
  }

  def execute(): (ArrowRecordBatch, String) = {
    var (vec, allocator) = write()
    var schemaRoot = read(vec, allocator)

    var schema = schemaRoot.getSchema
    val vectorUnloader = new VectorUnloader(schemaRoot)

    val recordBatch = vectorUnloader.getRecordBatch
    schemaRoot.close()
    return (recordBatch, schema.toJson)

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
    var t = new Test
    var (vec, allocator) = t.write()
    var schemaRoot = t.read(vec, allocator)

    println(">>>>TSV>>>>>>>" + schemaRoot.contentToTSVString())
    var schema = schemaRoot.getSchema
    val vectorUnloader = new VectorUnloader(schemaRoot)

    val recordBatch = vectorUnloader.getRecordBatch
    println("RECORD BARCH LENGTH: " + recordBatch.getLength)
    print("++++++++++++++++  " + recordBatch.getNodes.get(0).toString)
//    return recordBatch

    //.*********************************


    //...........................**********************************
    val outSchema = new ByteArrayOutputStream
    var channelSchema = new WriteChannel(Channels.newChannel(outSchema))
    var blockSchema = MessageSerializer.serialize(channelSchema, schema)
    println("Starting to publish...")
    val outBatch = new ByteArrayOutputStream
    var channelBatch = new WriteChannel(Channels.newChannel(outBatch))
    var blockBatch = MessageSerializer.serialize(channelSchema, recordBatch)


    println("***********************************************************")
    println("***********************************************************")
    var newAllocator:BufferAllocator = new RootAllocator(Long.MaxValue)
    val newSchemaRoot = VectorSchemaRoot.create(schema, newAllocator)



//    val inputStream = new ByteArrayInputStream(outBatch.toByteArray)
//    val channelIn = new ReadChannel(Channels.newChannel(inputStream))
//    val deserialized = MessageSerializer.deserializeMessageBatch(channelIn, newAllocator)
//    println("Deserialized Class: {}", deserialized.getClass)
//    var recordBatchIn = deserialized.asInstanceOf[ArrowRecordBatch]
//    newSchemaRoot.setRowCount(recordBatchIn.getLength)
//    newSchemaRoot.getFieldVectors.forEach(x => {
//      println("HELLLLLLLLLLLOOOOOOOOOOOOOOOOO>>>>>>>>>>>>>>>>>" + x.toString)
//    })

//
//    var loader = new VectorLoader(newSchemaRoot)
//    loader.load(recordBatch)

          val finalVectorsAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE)
          val newRoot = VectorSchemaRoot.create(schema, finalVectorsAllocator)
            val vectorLoader = new VectorLoader(newRoot)
            vectorLoader.load(recordBatch)
            val intReader = newRoot.getVector("emp_no").getReader

            var i = 0
            var count = 20
            while (i < count) {
              intReader.setPosition(i)
              println(intReader.readInteger().intValue())
              i += 1
            }










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