package de.tuberlin.dima.bdapro.muses.starter

class DataSource extends DataSourceBase {
  var dataSourceType = DataSourceTypes.RDBMS
  var properties: DataSourceProperties = null

  override def toString = s"DataSource(dataSourceType=$dataSourceType, properties=$properties)"
}