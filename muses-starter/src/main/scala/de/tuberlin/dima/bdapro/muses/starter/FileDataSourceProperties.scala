package de.tuberlin.dima.bdapro.muses.starter

class FileDataSourceProperties extends DataSourceProperties {
  var filePath:String = null
  var separator: String = ","
  //list other properties needed

  override def toString = s"FileDataSourceProperties(filePath=$filePath, separator=$separator)"
}
