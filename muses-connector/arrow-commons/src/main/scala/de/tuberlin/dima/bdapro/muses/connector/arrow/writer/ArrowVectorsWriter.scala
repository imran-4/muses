package de.tuberlin.dima.bdapro.muses.connector.arrow.writer

import java.math.BigDecimal
import java.nio.charset.Charset

import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat


object ArrowVectorsWriter {

  //  val allocator = new RootAllocator(Integer.MAX_VALUE)
  //
  //  //getter
  //  def getAllocator(): RootAllocator = allocator
  //
  //  def writeComplex(parentName: String, writerName: String, columnsNameAndTypes: List[Tuple2[String, String]]): Unit = {
  //    val parent = NonNullableStructVector.empty(parentName, allocator)
  //    val writer = new ComplexWriterImpl(writerName, parent)
  //    val rootWriter = writer.rootAsStruct
  //
  //    //////////////////???????????????????????????????????????
  //    columnsNameAndTypes.foreach(x => {
  //      if (x._2 == "INT") {
  //        var columnWriter = rootWriter.bigInt(x._1)
  //      }
  //    })

  //  }
  def writeFieldVector(fieldVector: FieldVector, obj: Object, index: Int): Unit = {
    //TODO: Implements for other types as well
    var fieldVectorValuesType = fieldVector.getField.getType

    if (fieldVectorValuesType.isInstanceOf[ArrowType.Int]) {
      ArrowVectorsWriter.writeIntData(fieldVector.asInstanceOf[IntVector], index, obj.asInstanceOf[Int])
    } else if (fieldVectorValuesType.isInstanceOf[ArrowType.Date]) {
      ArrowVectorsWriter.writeDateDayData(fieldVector.asInstanceOf[DateDayVector], index, obj.asInstanceOf[java.util.Date])
    } else if (fieldVectorValuesType.isInstanceOf[ArrowType.Utf8]) {
      ArrowVectorsWriter.writeVarCharData(fieldVector.asInstanceOf[VarCharVector], index, obj.toString)
    } else {
      throw new Exception("No corresponding vector available so far.")
    }
  }

  //NOTE: Keep in mind that there are different variants of setSafe() for each data type. may be we need to call those based on certain condition(s)

  def writeTimeStampData(vector: TimeStampVector, index: Int, value: Long): Unit = {
    vector.setSafe(index, value)
    vector.setValueCount(index)
  }

  def writeDecimalData(vector: DecimalVector, index: Int, value: BigDecimal): Unit = {
    vector.setSafe(index, value)
    vector.setValueCount(index)
  }

  def writeIntData(vector: IntVector, index: Int, value: Int): Unit = {
    vector.setSafe(index, value)
    vector.setValueCount(index)
  }

  def writeBitData(vector: BitVector, index: Int, value: Boolean): Unit = {
    vector.setSafe(index, if (value) 1 else 0)
    vector.setValueCount(index)
  }

  def writeIntervalYearData(vector: IntervalYearVector, index: Int, value: Int): Unit = {
    vector.setSafe(index, value)
    vector.setValueCount(index)
  }

  def writeIntervalDayData(vector: IntervalDayVector, index: Int, value: Int): Unit = {
    vector.setSafe(index, value, 100) //third argument is milliseconds
    vector.setValueCount(index)
  }

  def writeTimeSecData(vector: TimeSecVector, index: Int, value: Int): Unit = {
    vector.setSafe(index, value)
    vector.setValueCount(index)
  }

  def writeTimeMilliData(vector: TimeMilliVector, index: Int, value: Int): Unit = {
    vector.setSafe(index, value)
    vector.setValueCount(index)
  }

  def writeTimeMicroData(vector: TimeMicroVector, index: Int, value: Long): Unit = {
    vector.setSafe(index, value)
    vector.setValueCount(index)
  }

  def writeTimeNanoData(vector: TimeNanoVector, index: Int, value: Long): Unit = {
    vector.setSafe(index, value)
    vector.setValueCount(index)
  }

  def writeDateDayData(vector: DateDayVector, index: Int, value: java.util.Date): Unit = {
    var localDate = LocalDate.fromDateFields(value)
    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    val lvalue = localDate.toString(formatter)
    vector.asInstanceOf[DateDayVector].setSafe(index, Integer.parseInt(lvalue).intValue())
    vector.setValueCount(index)
  }

  def writeDateMilliData(vector: DateMilliVector, index: Int, value: Long): Unit = {
    vector.setSafe(index, value)
    vector.setValueCount(index)
  }

  def writeSmallIntData(vector: SmallIntVector, index: Int, value: Int): Unit = {
    vector.setSafe(index, value)
    vector.setValueCount(index)
  }

  def writeTinyIntData(vector: TinyIntVector, index: Int, value: Int): Unit = {
    vector.setSafe(index, value)
    vector.setValueCount(index)
  }

  def writeBigIntData(vector: BigIntVector, index: Int, value: Long): Unit = {
    vector.setSafe(index, value)
    vector.setValueCount(index)
  }

  def writeFloatData(vector: Float4Vector, index: Int, value: Float): Unit = {
    vector.setSafe(index, value)
    vector.setValueCount(index)
  }

  def writeDoubleData(vector: Float8Vector, index: Int, value: Double): Unit = {
    vector.setSafe(index, value)
    vector.setValueCount(index)
  }

  def writeVarBinaryData(vector: VarBinaryVector, index: Int, value: String): Unit = {
    val utf8Charset = Charset.forName("UTF-8")
    vector.setSafe(index, value.getBytes(utf8Charset))
    vector.setValueCount(index)
  }

  def writeVarCharData(vector: VarCharVector, index: Int, value: String): Unit = {
    val utf8Charset = Charset.forName("UTF-8")
    val bytes = value.getBytes(utf8Charset)
    vector.setSafe(index, bytes, 0, bytes.length)
    vector.setValueCount(index)
    vector.setValueLengthSafe(index, bytes.length)
  }

  //write also for complex data, dictionary, struct, list

  //following are not implemented yet, check if there is alternative available in the already implemented functions
  //  def writeFixedSizeBinaryData(): Unit = {}
  //  def writeTimeStampMicroTZData(): Unit = {}
  //  def writeTimeStampMicroData(): Unit = {}
  //  def writeTimeStampMilliTZData(): Unit = {}
  //  def writeTimeStampMilliData(): Unit = {}
  //  def writeTimeStampNanoTZData(): Unit = {}
  //  def writeTimeStampNanoData(): Unit = {}
  //  def writeTimeStampSecTZData(): Unit = {}
  //  def writeTimeStampSecData(): Unit = {}
  //  def writeUInt1Data(): Unit = {}
  //  def writeUInt2Data(): Unit = {}
  //  def writeUInt4Data(): Unit = {}
  //  def writeUInt8Data(): Unit = {}
}
