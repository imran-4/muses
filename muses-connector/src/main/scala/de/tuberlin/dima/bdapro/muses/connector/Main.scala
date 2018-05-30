package de.tuberlin.dima.bdapro.muses.connector

import java.io.{File, FileOutputStream}

import org.apache.arrow.memory.{AllocationListener, BufferAllocator, BufferManager, RootAllocator}
import org.apache.arrow.memory
import org.apache.arrow.vector.{DecimalVector, FieldVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl
import org.apache.arrow.vector.complex.writer.VarCharWriter
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}

import scala.io.Source

object Main {
  def main(args: Array[String]): Unit = {
    val csvLines: Stream[Array[String]] = Source.fromFile(new File("/home/mi/abc.csv")).getLines.toStream.map(line => line.split(","))

    var allocator:BufferAllocator = new RootAllocator(AllocationListener.NOOP, Long.MaxValue)
    
    //DecimalVector.class, "decimal", new ArrowType.Decimal(10, scale), allocator
    var scale=3

    var intValues = new Array[Long](60)
    var i = 0
    while ( {
      i < intValues.length / 2
    }) {
      intValues(i) = 1 << i + 1
      intValues(2 * i) = -1 * (1 << i + 1)

      {
        i += 1; i - 1
      }
    }

    var decimalVector =  classOf[DecimalVector].cast(FieldType.nullable(new ArrowType.Decimal(10, scale)).createNewSingleVector("decimal", allocator, null))

    import org.apache.arrow.vector.DecimalVector
    try {
      val oldConstructor = new DecimalVector("decimal", allocator, 10, scale)
      try {
        println(decimalVector.getField.getType)
        println(oldConstructor.getField.getType)
        println(oldConstructor.equals(decimalVector))
      }
      finally if (oldConstructor != null) oldConstructor.close()

    }

    decimalVector.allocateNew
    var values:Array[BigDecimal] = new Array[BigDecimal](intValues.length)
    import java.math.BigInteger
    import java.math.BigDecimal

    //write values
    i = 0
    while ( {  i < intValues.length
    }) {

      val decimal = new BigDecimal(BigInteger.valueOf(intValues(i)), scale)
      values(i) = decimal
      decimalVector.setSafe(i, decimal)

      {
        i += 1; i - 1
      }
    }

    //read the written values
    println("--------------------------------------")
    i = 0
    decimalVector.setValueCount(intValues.length)
    while ( {
      i < intValues.length
    }) {
      val value = decimalVector.getObject(i)
      println("###")
      println("index: " + i)
      println("values(i): " + values(i))
      println("value: " + value)

      {
        i += 1; i - 1
      }
    }
//
//    allocator.verify()
//
//    allocator.close()


  }


//  def main(args: Array[String])  {
//    // Open stream of rows
//    var allocator = new RootAllocator(Integer.MAX_VALUE)
//    val csvLines: Stream[Array[String]] = Source.fromFile(new File("/home/mi/abc.csv")).getLines.toStream.map(line => line.split(","))
//
//
//    // Define a parent to hold the vectors
//    val parent = StructVector.empty("parent", allocator)
//    // Create a new writer. VarCharWriterImpl would probably do as well?
//    val writer = new ComplexWriterImpl("root", parent)
//
//    // Initialise a writer for each column, using the header as the name
//    val rootWriter = writer.rootAsStruct()
//
//    val writers = csvLines.head.map(colName =>
//      rootWriter.varChar(colName))
//
//    Stream.from(0)
//      .zip(csvLines.tail) // Zip the rows with their index
//      .foreach( rowTup => { // Iterate on each (index, row) tuple
//      val (idx, row) = rowTup
//      Range(0, row.size) // Iterate on each field of the row
//        .foreach(column =>
//        Option(row(column)) // row(column) may be null,
//          .foreach(str =>  // use the option as a null check
//          write1(writers(column), idx, allocator, str)
//        )
//      )
//    }
//    )
//
//    toFile(parent.getChild("root"), "/home/mi/csv111.txt") // Save everything to a file
//  }
//
//
//  def write1(writer: VarCharWriter, idx: Int, allocator: BufferAllocator, data: String): Unit = {
//    // Set the position to the correct index
//    writer.setPosition(idx)
//    val bytes = data.getBytes()
//    // Apparently the allocator is required again to build a new buffer
//    val varchar = allocator.buffer(bytes.length)
//    varchar.setBytes(0, data.getBytes())
//    writer.writeVarChar(0, bytes.length, varchar)
//  }
//
//  def toFile(parent: FieldVector, fName: String): Unit = {
//    // Extract a schema from the parent: that's the part I struggled with in the original question
//    val rootSchema = new VectorSchemaRoot(parent)
//    val stream = new FileOutputStream(fName)
//    val fileWriter = new ArrowFileWriter(
//      rootSchema,
//      null, // We don't use dictionary encoding.
//      stream.getChannel)
//    // Write everything to file...
//    fileWriter.start()
//    fileWriter.writeBatch()
//    fileWriter.end()
//    stream.close()
//  }

}