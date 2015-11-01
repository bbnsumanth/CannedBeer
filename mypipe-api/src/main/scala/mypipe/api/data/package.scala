package mypipe.api

import java.lang.{ Long ⇒ JLong }

package object data {

  case class PrimaryKey(columns: List[ColumnMetadata])

  /** @param name column name
   *  @param colType column type which is a custom enum value
   *  @param isPrimaryKey
   */
  case class ColumnMetadata(name: String, colType: ColumnType.EnumVal, isPrimaryKey: Boolean)

  /** @param table :Table, it contains table name,id ,db,list[columnMetadata],primary key info etc
   *  @param columns : Column, it cointains metadata for a column and value of that column as serializable obj
   */
  case class Row(table: Table, columns: Map[String, Column])

  /** @param id :table id
   *  @param name :table name
   *  @param db :db name
   *  @param columns :list of columnMetadata
   *  @param primaryKey :List of columnMetadata wich are primary keys
   */
  case class Table(id: JLong, name: String, db: String, columns: List[ColumnMetadata], primaryKey: Option[PrimaryKey])

  class UnknownTable(override val id: JLong, override val name: String, override val db: String) extends Table(id, name, db, columns = List.empty, primaryKey = None)

  /** @param metadata column metadata
   *  @param value value of the column as a serializable value
   */
  case class Column(metadata: ColumnMetadata, value: java.io.Serializable = null) {

    def value[T]: T = {
      value match {
        case null ⇒ null.asInstanceOf[T]
        case v    ⇒ v.asInstanceOf[T]
      }
    }

    def valueOption[T]: Option[T] = {
      value match {
        case null ⇒ None
        case v    ⇒ Some(v.asInstanceOf[T])
      }
    }
  }

}
