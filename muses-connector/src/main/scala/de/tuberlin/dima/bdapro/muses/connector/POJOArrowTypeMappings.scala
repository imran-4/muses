package de.tuberlin.dima.bdapro.muses.connector

import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}
import org.apache.arrow.vector.types.pojo.ArrowType

class POJOArrowTypeMappings {

  //pojoArrowTypeMappings: POJOArrowTypeMappings is specified as a parameter so the user can be sure of lengths, zones, units etc..
  def getArrowType(typeName: String, pojoArrowTypeMappings: POJOArrowTypeMappings): ArrowType = {
    val currentType = typeName.toUpperCase
    val arrowType:ArrowType = currentType match {
      case "INT" | "INTEGER" | "LONG" | "UINT" | "SHORT" | "USHORT"  => new ArrowType.Int(POJOArrowTypeMappings.Integer_Size, POJOArrowTypeMappings.Is_Integer_Signed)
      case "DATE"  => new ArrowType.Date(DateUnit.DAY)
      case "VARCHAR" | "CHAR"  => new ArrowType.Utf8()
      case "FLOAT"  => new ArrowType.FloatingPoint(POJOArrowTypeMappings.Float_Precision)
      case "DECIMAL"  => new ArrowType.Decimal(POJOArrowTypeMappings.Decimal_Precision, POJOArrowTypeMappings.Decimal_Scale)
      case "BOOLEAN" | "BOOL" => new ArrowType.Bool
      case "NULL" | "NONE"  => new ArrowType.Null
      case "STRUCT" | "CLASS" | "DICTIONARY" => new ArrowType.Struct

      //not sure about type names (on the left side) used for sources
      case "BINARY"  => new ArrowType.Binary
      case "FIXEDSIZEBINARY"  => new ArrowType.FixedSizeBinary(POJOArrowTypeMappings.Fixed_Size_Binary_Byte_Width)
      case "FIXEDSIZELIST"  => new ArrowType.FixedSizeList(POJOArrowTypeMappings.Fixed_Size_List_Size)
      case "INTERVAL"  => new ArrowType.Interval(POJOArrowTypeMappings.Interval_Unit)
      case "LIST"  => new ArrowType.List
      case "TIME"  => new ArrowType.Time(POJOArrowTypeMappings.Time_Unit_For_Time, POJOArrowTypeMappings.BitWidth_For_Time)
      case "TIMESTAMP"  => new ArrowType.Timestamp(POJOArrowTypeMappings.Time_Unit_For_TimeStamp, POJOArrowTypeMappings.Time_Zone)
//    case "UNION"  => new ArrowType.Union()
      case _  => throw new Exception(typeName + " is not supported yet.") // the default, catch-all
    }
    return arrowType
  }
}

object POJOArrowTypeMappings {
  var Is_Integer_Signed = true
  var Integer_Size:Int = 32
  var Time_Zone:String = "CEST"
  var Time_Unit_For_Time:TimeUnit = TimeUnit.MICROSECOND
  var Time_Unit_For_TimeStamp:TimeUnit = TimeUnit.MICROSECOND
  var Interval_Unit:IntervalUnit = IntervalUnit.DAY_TIME
  var Float_Precision:FloatingPointPrecision = FloatingPointPrecision.DOUBLE
  var Decimal_Precision:Int = 10
  var Decimal_Scale:Int = 3
  var Date_Unit:DateUnit = DateUnit.DAY
  var Fixed_Size_Binary_Byte_Width:Int = 8
  var Fixed_Size_List_Size:Int = 0
  var BitWidth_For_Time = 1
}
