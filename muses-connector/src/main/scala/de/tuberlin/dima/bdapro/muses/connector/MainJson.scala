package de.tuberlin.dima.bdapro.muses.connector

import java.io.File

import io.netty.buffer.ArrowBuf
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.impl.NullableStructReaderImpl
import org.apache.arrow.vector.complex.reader.BaseReader.ComplexReader
import org.apache.arrow.vector.holders.VarCharHolder

object MainJson {
  def main(args: Array[String]): Unit = {
    import org.apache.arrow.vector.VectorSchemaRoot
    import org.apache.arrow.vector.complex.StructVector
    import org.apache.arrow.vector.ipc.JsonFileReader
    val file = new File("/home/mi/abc.json")

    import org.apache.arrow.memory.RootAllocator

    var allocator: BufferAllocator = new RootAllocator(Integer.MAX_VALUE)
    val COUNT = 10
    val count = COUNT

    // write
    var parent:StructVector = null
    try {
      val originalVectorAllocator = allocator.newChildAllocator("original_vectors", 0, Integer.MAX_VALUE)
      parent = StructVector.empty("parent", originalVectorAllocator)
      try {
        writeData(count, parent)
        writeJSON(file, new VectorSchemaRoot(parent.getChild("root")), null)
      } finally {

//        if (originalVectorAllocator != null) originalVectorAllocator.close()
        //if (parent != null) parent.close()
      }
    }

    // read
    try {


      var x = parent.getChild("root")
      var y = new VectorSchemaRoot(x)
      var sch = y.getSchema
      y.getVector("")



      val readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE)

     val r =  new VectorSchemaRoot(parent.getChild("root"))





      val reader = new JsonFileReader(file, readerAllocator)
      try {
        val schema = reader.start
        println("reading schema: " + schema)
        // initialize vectors
        try {
          val root = reader.read
          try
            validateContent(count, root)
          finally if (root != null) root.close()
        }
      } finally {
        if (readerAllocator != null) readerAllocator.close()
        if (reader != null) reader.close()
      }
    }
  }

  import org.apache.arrow.vector.complex.StructVector
  import org.apache.arrow.vector.complex.impl.ComplexWriterImpl


  protected def writeData(count: Int, parent: StructVector): Unit = {
    val writer = new ComplexWriterImpl("root", parent)
    val rootWriter = writer.rootAsStruct
    val intWriter = rootWriter.integer("int")
    val bigIntWriter = rootWriter.bigInt("bigInt")
    val float4Writer = rootWriter.float4("float")



    import io.netty.buffer.ArrowBuf
    var i = 0
    while ( {
      i < count
    }) {
      intWriter.setPosition(i)
      intWriter.writeInt(i)
      bigIntWriter.setPosition(i)
      bigIntWriter.writeBigInt(i)
      float4Writer.setPosition(i)

      float4Writer.writeFloat4(if (i == 0) Float.NaN
      else i)

      {
        i += 1; i - 1
      }
    }
    writer.setValueCount(count)
  }

  import org.apache.arrow.vector.VectorSchemaRoot
  import org.apache.arrow.vector.dictionary.DictionaryProvider
  import org.apache.arrow.vector.ipc.JsonFileWriter
  import java.io.IOException

  @throws[IOException]
  def writeJSON(file: File, root: VectorSchemaRoot, provider: DictionaryProvider): Unit = {
    val writer = new JsonFileWriter(file, JsonFileWriter.config.pretty(true))
    writer.start(root.getSchema, provider)





    writer.write(root)
    writer.close()
  }

  import org.apache.arrow.vector.VectorSchemaRoot

  def validateContent(count: Int, root: VectorSchemaRoot): Unit = {
      assert(count == root.getRowCount)
    var i = 0
    while ( {
      i < count
    }) {

      assert(i == root.getVector("int").getObject(i))
      assert(i == root.getVector("bigInt").getObject(i))

      {
        i += 1; i - 1
      }
    }
  }

}
