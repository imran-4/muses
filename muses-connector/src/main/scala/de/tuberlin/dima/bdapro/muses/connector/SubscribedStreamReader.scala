package de.tuberlin.dima.bdapro.muses.connector

import java.io.{BufferedWriter, ByteArrayInputStream, File, FileWriter}
import java.nio.channels.Channels
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, MessageSerializer}
import org.apache.arrow.vector.types.pojo.{ArrowType, Schema}

class SubscribedStreamReader {
  val alloc = new RootAllocator(Long.MaxValue)
  var schema:Schema = null

  def createSchema(schemaJson: String) : Unit = {
    this.schema = Schema.fromJSON(schemaJson)
  }

  //https://github.com/apache/arrow/blob/611a4b951e24f4f967c3d382a2027dc035fc37f0/java/vector/src/test/java/org/apache/arrow/vector/ipc/TestArrowReaderWriter.java
  def loadVector(byteArray: Array[Byte]): Unit = {
    //val finalVectorsAllocator = alloc.newChildAllocator("final vectors", 0, Integer.MAX_VALUE)
    val inputStream = new ByteArrayInputStream(byteArray)
    val channel = new ReadChannel(Channels.newChannel(inputStream))
    val deserialized = MessageSerializer.deserializeMessageBatch(channel, this.alloc)
    var recordBatch = deserialized.asInstanceOf[ArrowRecordBatch]

    println("RECORD BATCH:   " + recordBatch.toString)

    val newRoot = VectorSchemaRoot.create(this.schema, this.alloc)
    val vectorLoader = new VectorLoader(newRoot)
    vectorLoader.load(recordBatch)

    println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    //////////////////////////////////
    val file = new File("/home/mi/Desktop/subscriber.csv")
    val bw = new BufferedWriter(new FileWriter(file))

    var fieldsString: String = ""
    newRoot.getSchema.getFields.forEach(x => fieldsString += x.getName + ",")
    fieldsString += "\r\n"
    bw.write(fieldsString)

    /////////////////////////////////

    var count = newRoot.getRowCount
    var i=0
    while (i < count) {
      var str: StringBuilder = new StringBuilder
      newRoot.getFieldVectors.forEach(x => {
        var fieldName = x.getField.getName
        var fieldType = x.getField.getType

        if (fieldType.isInstanceOf[ArrowType.Int]) {
          println("INT COUNT: " + x.getValueCount)
          println("FIELDNAME>>>>>>>" + fieldName)
          var value = x.asInstanceOf[IntVector].getObject(i)
          println("VALUE: " + value)
          str.append(value + ",")
        } else if (fieldType.isInstanceOf[ArrowType.Date]) {
          println("DATE COUNT: " + x.getValueCount)
          println("FIELDNAME>>>>>>>" + fieldName)

          var value = x.asInstanceOf[DateDayVector].getObject(i)

          println("VALUE: " + value)
          str.append(value + ",")
        } else if (fieldType.isInstanceOf[ArrowType.Utf8]) {
          println("UTF COUNT: " + x.getValueCount)
          println("FIELDNAME>>>>>>>" + fieldName)

          var value = x.asInstanceOf[VarCharVector].getObject(i)
          println("VALUE: " + value)
          str.append(value + ",")
        }
      })
      i += 1
      str.append("\r\n")
      bw.write(str.toString())
    }
    bw.flush()
    bw.close()
    println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")

//    println("TSV:::::::" + newRoot.contentToTSVString())
  }
}