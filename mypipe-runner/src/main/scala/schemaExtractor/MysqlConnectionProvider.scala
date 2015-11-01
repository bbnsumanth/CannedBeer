package schemaExtractor

import javax.management.RuntimeErrorException
import javax.sql.DataSource

import com.mchange.v2.c3p0.ComboPooledDataSource

import scala.collection.concurrent.TrieMap
/** Created by sumanth_bbn on 28/10/15.
 */
object MysqlConnectionProvider {
  val DEFAULT_MYSQL_JDBC_DRIVER = "com.mysql.jdbc.Driver"
  val dataSources = new TrieMap[String, DataSource]()

  def createDataSource(dataSourceConfig: DataSourceConfig): DataSource = {
    val cpds = new ComboPooledDataSource()
    cpds.setDriverClass(DEFAULT_MYSQL_JDBC_DRIVER)
    cpds.setJdbcUrl(dataSourceConfig.getJDBCUrl)
    cpds.setMinPoolSize(dataSourceConfig.minPoolSize)
    cpds.setMaxPoolSize(dataSourceConfig.maxPoolSize)
    cpds.setTestConnectionOnCheckin(true)
    cpds

  }

  def addDataSource(dataSourceConfig: DataSourceConfig) = {
    dataSources.putIfAbsent(dataSourceConfig.dbName, createDataSource(dataSourceConfig))
  }

  def getConnection(dbName: String) = {
    dataSources.contains(dbName) match {
      case true  ⇒ dataSources.get(dbName).get.getConnection
      case false ⇒ throw new RuntimeErrorException(null, "No dataSouce found for dataSourceId : " + dbName)
    }
  }

}
