package de.tuberlin.dima.bdapro.muses.connector.arrow.writer

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import java.sql.ResultSet
import java.util

import com.google.common.collect.ImmutableList
import de.tuberlin.dima.bdapro.muses.connector.arrow.POJOArrowTypeMappings
import de.tuberlin.dima.bdapro.muses.connector.rdbms.connectionmanager._
import de.tuberlin.dima.bdapro.muses.connector.rdbms.reader.DataReader
import org.apache.arrow.memory.{AllocationListener, RootAllocator}
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class Writer {
  private val allocator = new RootAllocator(AllocationListener.NOOP, Long.MaxValue)
  private var vectorSchemaRoot:VectorSchemaRoot = null

  private def fieldx(name: String, nullable: Boolean, typ: ArrowType, children: Field*) = new Field(name, new FieldType(nullable, typ, null, null), children.toList.asJava)

  private def field(na: String, typ: ArrowType, chi: Field*) = fieldx(na, true, typ, chi: _*)

  //TODO: LATER MOVE IT TO RELATED PROJECT OF JDBC
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

  def write(data: ResultSet, columns: util.ArrayList[Tuple2[String, String]]): Unit = {
//    val (it, columns) = readDatabase()

    var parentBuilder: ImmutableList.Builder[Field] = ImmutableList.builder()
    columns.forEach(column => {
      var createdField = field(column._1, POJOArrowTypeMappings.getArrowType(column._2))
      parentBuilder.add(createdField)
    })

    val fieldVectors = new util.ArrayList[FieldVector]
    var schema = new Schema(parentBuilder.build)

    for (_field <- schema.getFields) {
      val vector = _field.createVector(allocator)
      vector.allocateNew()
      fieldVectors.add(vector)
    }

    var count = -1
    while (data.next()) {
      count += 1
      fieldVectors.forEach(field => {
        var name = field.getField.getName
        var obj = data.getObject(name)
        ArrowVectorsWriter.writeFieldVector(field, obj, count)
      })

    }

    this.vectorSchemaRoot = new VectorSchemaRoot(schema.getFields(), fieldVectors, count+1)
  }

  def getAllocator(): RootAllocator = this.allocator

  def getVectorSchemaRoot(): VectorSchemaRoot = this.vectorSchemaRoot

  def getSchemaJson() = this.vectorSchemaRoot.getSchema.toJson

  def getByteArrayOutputStream(): ByteArrayOutputStream = {
    var schema = this.vectorSchemaRoot.getSchema
    val vectorUnloader = new VectorUnloader(this.vectorSchemaRoot)
    val recordBatch = vectorUnloader.getRecordBatch

    val outStream = new ByteArrayOutputStream
    var channel = new WriteChannel(Channels.newChannel(outStream))
    var block = MessageSerializer.serialize(channel, recordBatch)

    return outStream
  }
}
