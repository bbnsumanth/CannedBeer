package schemaExtractor

import java.sql._

import schemaModifier.{ Field, ColumnBuilder }

import scala.collection.mutable

/** Created by sumanth_bbn on 28/10/15.
 */
object MysqlUtils {
  /** Fields fetch query */
  private val FIELDS_FETCH_QUERY: String = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? and TABLE_NAME = ?"
  /** Tables fetch query */
  private val TABLES_FETCH_QUERY: String = "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?"
  /** Table check query */
  private val TABLE_CHECK_QUERY: String = "SELECT count(*) AS count FROM "
  /** Field details fetch query */
  private val FIELDS_DETAILS_FETCH_QUERY: String = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? and TABLE_NAME = ?"

  /** Release db resource.
   *  @param connection object
   *  @param resultSet Object
   *  @param statement Object
   */
  def releaseDBResource(connection: Connection, resultSet: ResultSet, statement: Statement) {
    try {
      if (connection != null)
        connection.close()
    } catch {
      case e: Exception ⇒ e.printStackTrace()
    }
    try {
      if (resultSet != null)
        resultSet.close()
    } catch {
      case e: Exception ⇒ e.printStackTrace()
    }
    try {
      if (statement != null)
        statement.close()
    } catch {
      case e: Exception ⇒ e.printStackTrace()
    }
  }

  /** Get the tables in the current database.
   *  @param dbName the db name
   *  @return list of tables in the current db
   */
  def getTablesInDB(dbName: String): List[String] = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    val list = new mutable.MutableList[String]()

    try {
      connection = MysqlConnectionProvider.getConnection(dbName)
      preparedStatement = connection.prepareStatement(TABLES_FETCH_QUERY)
      preparedStatement.setString(1, dbName)
      resultSet = preparedStatement.executeQuery
      while (resultSet.next()) {
        list += (resultSet.getString("table_name"))
      }

    } catch {
      case e: SQLException ⇒ System.out.println("ERROR: Unable to fetch the table names in the database specified: " + e.toString)
    } finally {
      releaseDBResource(connection, resultSet, preparedStatement)
    }

    list.toList
  }

  /** Gets the primary keys.
   *  @param dbName the dataSourceId
   *  @param tableName the table name
   *  @return the primary keys
   */
  def getPrimarykeys(dbName: String, tableName: String): List[String] = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    val list = new mutable.MutableList[String]()
    try {
      connection = MysqlConnectionProvider.getConnection(dbName)
      val primaryKeyFetchQuery: String = "SHOW index FROM " + tableName + " WHERE Key_name = 'PRIMARY'"
      preparedStatement = connection.prepareStatement(primaryKeyFetchQuery)
      resultSet = preparedStatement.executeQuery
      while (resultSet.next) {
        list += (resultSet.getString("Column_name"))
      }
    } catch {
      case e: Exception ⇒ e.printStackTrace
    } finally {
      releaseDBResource(connection, resultSet, preparedStatement)
    }
    list.toList
  }

  /** Gets the fields in table.
   *  @param dbName the database
   *  @param table the table
   *  @return the fields in table
   */
  def getFieldsInTable(dbName: String, table: String): List[String] = {

    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    val list = new mutable.MutableList[String]()
    try {
      connection = MysqlConnectionProvider.getConnection(dbName)
      preparedStatement = connection.prepareStatement(FIELDS_FETCH_QUERY)
      preparedStatement.setString(1, dbName)
      preparedStatement.setString(2, table)
      resultSet = preparedStatement.executeQuery
      while (resultSet.next) {
        list += (resultSet.getString("COLUMN_NAME"))
      }
    } catch {
      case e: SQLException ⇒ {
        System.out.println("Unable to determine the fields from the given table: " + e.toString)
      }
    } finally {
      releaseDBResource(connection, resultSet, preparedStatement)
    }

    list.toList
  }

  /** checks if the current table is a valid table in the given schema.
   *  @param dbName the dataSourceId
   *  @param table : table name
   *  @return true if valid table, false otherwise
   */
  def isValidTable(dbName: String, table: String): Boolean = {
    var bool = false
    var connection: Connection = null
    var resultSet: ResultSet = null
    var preparedStatement: PreparedStatement = null
    try {
      connection = MysqlConnectionProvider.getConnection(dbName)
      preparedStatement = connection.prepareStatement(TABLE_CHECK_QUERY + table)
      resultSet = preparedStatement.executeQuery
      if (resultSet.next && resultSet.getInt("count") >= 0) {
        bool = true
      }
    } catch {
      case e: SQLException ⇒ {
        System.out.println("ERROR: Unable to determine if it's a valid schema : " + e.toString)
        bool = false
        return false
      }
    } finally {
      releaseDBResource(connection, resultSet, preparedStatement)
    }
    bool
  }

  /** Checks if the field is present in the table.
   *  @param dbName the database
   *  @param field The field to check if it's valid
   *  @param table the table
   *  @return true, if is valid field
   */
  def isValidField(dbName: String, field: String, table: String): Boolean = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      connection = MysqlConnectionProvider.getConnection(dbName)
      preparedStatement = connection.prepareStatement("SELECT count(COLUMN_NAME)  as count FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? and TABLE_NAME = ? and COLUMN_NAME = ?")
      preparedStatement.setString(1, dbName)
      preparedStatement.setString(2, table)
      preparedStatement.setString(3, field)
      resultSet = preparedStatement.executeQuery
      return (resultSet.next && resultSet.getInt("count") > 0)
    } catch {
      case e: SQLException ⇒ {
        System.out.println("ERROR: Unable to determine if it's a valid field ( " + field + "): " + e.toString)
        return false
      }
    } finally {
      releaseDBResource(connection, resultSet, preparedStatement)
    }
  }

  /** Gets the field details.
   *  @param dbName the db name
   *  @param table the table name
   *  @return the field details
   */
  def getFieldDetails(dbName: String, table: String): List[Field] = {
    val list = new mutable.MutableList[Field]()
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      connection = MysqlConnectionProvider.getConnection(dbName)
      preparedStatement = connection.prepareStatement(FIELDS_DETAILS_FETCH_QUERY)
      preparedStatement.setString(1, dbName)
      preparedStatement.setString(2, table)
      resultSet = preparedStatement.executeQuery
      while (resultSet.next) {
        val column = ColumnBuilder.getColumn(resultSet.getString("COLUMN_NAME"), resultSet.getString("DATA_TYPE"), resultSet.getInt("ORDINAL_POSITION"))
        list += (column)
      }
    } catch {
      case e: SQLException ⇒ {
        e.printStackTrace
      }
    } finally {
      releaseDBResource(connection, resultSet, preparedStatement)
    }
    list.toList
  }

}
