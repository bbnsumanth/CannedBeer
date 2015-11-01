package schemaModifier

/** Created by sumanth_bbn on 28/10/15.
 */

case class Field(name: String, `type`: Array[String])

object ColumnBuilder {
  def getColumn(name: String, dataType: String, position: Int) = {
    val datatypeList = Array(MysqlToAvroMapper.valueOf(dataType.toUpperCase).getAvroType, "null")
    new Field(name, datatypeList)
  }
}
object HeaderBuilder {
  def getHeader(name: String, typeF: String) = {
    val datatypeList = Array(typeF, "null")
    new Field(name, datatypeList)
  }
}
