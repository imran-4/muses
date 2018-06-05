package de.tuberlin.dima.bdapro.muses.connector

import org.apache.arrow.memory._
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.complex._
import org.apache.arrow.vector.complex.impl.{ComplexWriterImpl, NullableStructReaderImpl}

object MainJson2 {
  def main(args: Array[String]): Unit = {

    val allocator: BufferAllocator = new RootAllocator(Integer.MAX_VALUE)

    var originalVectorAllocator = allocator.newChildAllocator("original", 0 , Integer.MAX_VALUE)

    val parent = StructVector.empty("parent", originalVectorAllocator)

    //---------------------------------------
    //writeData

    var writer = new ComplexWriterImpl("root", parent)
    var rootWriter = writer.rootAsStruct

    val idWriter = rootWriter.integer("id")
//    val fNameWriter = rootWriter.varChar("firstName")
//    val lNameWriter = rootWriter.varChar("lastName")
    val salarytWriter = rootWriter.float8("salary")

    idWriter.setPosition(0)
    idWriter.writeInt(155)
//
//
//    fNameWriter.setPosition(0)
//    fNameWriter.writeVarChar(1, 10, buffer)
//
//    lNameWriter.setPosition(0)
//    lNameWriter.writeVarChar(1, 10, buffer)

    salarytWriter.setPosition(0)
    salarytWriter.writeFloat8(2356.0)


    writer.setValueCount(1)


    /*************************/
    //parent.getChild("root").getChildrenFromFields.get(0).getReader.readObject()
      /************************/

    //val byteBufAllocator = allocator.getAsByteBufAllocator
    val child = parent.getChild("root")
    var vectors = child.getChildrenFromFields
    var totalVectors = vectors.size()
    var i = 0
    while (i < totalVectors) {
      val fieldVec = vectors.get(i)
      val fieldReader = fieldVec.getReader
      val data = fieldReader.readObject()
      println(data)
      i+=1
    }

//    val schemaRoot = new VectorSchemaRoot(child)
//    val fieldVectors = schemaRoot.getFieldVectors
//    val structReader = new NullableStructReaderImpl(parent)
//
//    val fields = schemaRoot.getFieldVectors

    //val fields1 = child.getChildrenFromFields
//    fields.forEach(x=> {
//      schemaRoot.getVector(x)
//    })

    println(allocator.getAllocatedMemory)
println()
  }
}
