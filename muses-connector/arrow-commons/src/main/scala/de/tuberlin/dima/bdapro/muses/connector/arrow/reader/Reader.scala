package de.tuberlin.dima.bdapro.muses.connector.arrow.reader

import java.io.{BufferedWriter, ByteArrayInputStream, File, FileWriter}
import java.nio.channels.Channels

import org.apache.arrow.memory.{AllocationListener, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, MessageSerializer}
import org.apache.arrow.vector.types.pojo.{ArrowType, Schema}


class Reader {
  private val allocator = new RootAllocator(AllocationListener.NOOP, Long.MaxValue)
  private var vectorSchemaRoot:VectorSchemaRoot = null
  private var schema: Schema = null

  def createSchema(schemaJson: String): Unit = {
    this.schema = Schema.fromJSON(schemaJson)
  }

  def read(recordBatchByteArray: Array[Byte]): Unit = {
    val inputStream = new ByteArrayInputStream(recordBatchByteArray)
    val channel = new ReadChannel(Channels.newChannel(inputStream))
    val deserializedMessage = MessageSerializer.deserializeMessageBatch(channel, this.allocator)
    var recordBatch = deserializedMessage.asInstanceOf[ArrowRecordBatch]

    this.vectorSchemaRoot = VectorSchemaRoot.create(this.schema, this.allocator)
    val vectorLoader = new VectorLoader(this.vectorSchemaRoot)
    vectorLoader.load(recordBatch)

    //TODO: MOVE IT TO RELATED PROJECT WHEN REFACTORING
    val file = new File("/home/mi/Desktop/subscriber.csv")
    val bw = new BufferedWriter(new FileWriter(file))

    var fieldsString: String = ""
    this.vectorSchemaRoot.getSchema.getFields.forEach(x => fieldsString += x.getName + ",")
    fieldsString += "\r\n"
    bw.write(fieldsString)

    var count = this.vectorSchemaRoot.getRowCount
    var i=0
    while (i < count) {
      var str: StringBuilder = new StringBuilder
      this.vectorSchemaRoot.getFieldVectors.forEach(x => {
        var fieldName = x.getField.getName
        var fieldType = x.getField.getType

        if (fieldType.isInstanceOf[ArrowType.Int]) {
          var value = x.asInstanceOf[IntVector].getObject(i)
          str.append(value + ",")
        } else if (fieldType.isInstanceOf[ArrowType.Date]) {
          var value = x.asInstanceOf[DateDayVector].getObject(i)
          str.append(value + ",")
        } else if (fieldType.isInstanceOf[ArrowType.Utf8]) {
          var value = x.asInstanceOf[VarCharVector].getObject(i)
          str.append(value + ",")
        }
      })
      i += 1
      str.append("\r\n")
      bw.write(str.toString())
    }
    bw.flush()
    bw.close()

    println("TSV: " + this.vectorSchemaRoot.contentToTSVString)
  }
}
