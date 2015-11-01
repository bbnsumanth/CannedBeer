package schemaGenerator

import java.io._

import com.fasterxml.jackson.core.{ JsonFactory, JsonGenerator }
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import org.codehaus.jackson.map.{ObjectMapper}
//import org.codehaus.jackson.{JsonFactory, JsonGenerator}
import schemaExtractor.{ DataSourceConfig, MysqlConnectionProvider, MysqlUtils }
import schemaModifier.{ Field, HeaderBuilder, Table }

import scala.collection.mutable

/** Created by sumanth_bbn on 29/10/15.
 */
class Generator(configList: List[DataSourceConfig],
                tablesIncluded: Map[String, List[String]] = null,
                tablesExcluded: Map[String, List[String]] = null) {

  val objectMapper: ObjectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  val jsonFactory: JsonFactory = new JsonFactory()
  val configs: List[DataSourceConfig] = configList
  val included = tablesIncluded
  val excluded = tablesExcluded
  //this is also part of constructor
  configs.map(conf ⇒ {
    addDataSources(conf)
  })

  def addDataSources(dsc: DataSourceConfig) = {
    MysqlConnectionProvider.addDataSource(dsc)
    println(s"datasource added for ${dsc.dbName}")
  }

  /** Generate schema for all tables of the given dataSourceId.
   *  @param dbName the db name
   *  @return table to schema mapping
   *  @throws IOException Signals that an I/O exception has occurred.
   */
  @throws(classOf[IOException])
  def generateSchemaForAllTablesInDB(dbName: String) = {
    //val tableNameToSchemaMap: mutable.HashMap[String, String] = new mutable.HashMap[String, String]
    val tableNameList: List[String] = MysqlUtils.getTablesInDB(dbName)
    //tableNameList.foreach(println)

    tableNameList.foreach(tableName ⇒ {
      //tableNameToSchemaMap.put(tableName, generateSchema(dbName, tableName))
      generateSchema(dbName, tableName)
      /*
      val boolean = (excluded.get(dbName) != null && excluded.get(dbName).contains(tableName)) || (included.get(dbName) != null && !included.get(dbName).contains(tableName))
      boolean match {
        case true =>
        case false => tableNameToSchemaMap.put(tableName, generateSchema(dbName, tableName))
      }
      */
    })

    //tableNameToSchemaMap.toMap
  }

  /** Generate schema for the given table.
   *  @param dbName the db name
   *  @param tableName tableName
   *  @return the string
   *  @throws IOException Signals that an I/O exception has occurred.
   */
  @throws(classOf[IOException])
  @throws(classOf[IllegalArgumentException])
  def generateSchema(dbName: String, tableName: String) = {

    /*
    val boolean = (excluded.get(dbName) != null && excluded.get(dbName).contains(tableName)) || (included.get(dbName) != null && !included.get(dbName).contains(tableName))
    boolean match {
      case true => throw new IllegalArgumentException("table is excluded for the schema generation in the settings")
      case false => {
        val columns: List[Column] = MysqlUtils.getFieldDetails(dbName,tableName)
        val primarykeys: List[String] = MysqlUtils.getPrimarykeys(dbName,tableName)
        val namespace = "roadrunnr.cannedbeer.avrogen"
        val `type` = "record"
        val name = s"${dbName}_${tableName}"
        val doc = s"Auto-generated Avro schema for ${dbName}_$tableName "
        val headers = List(HeaderBuilder.getHeader("database","string"),
          HeaderBuilder.getHeader("table","string"),
          HeaderBuilder.getHeader("tableId","long"),
          HeaderBuilder.getHeader("mutation","string"))
        val fields = headers ::: columns

        val record = Table(namespace,`type`,name,fields)

        val writer: StringWriter = new StringWriter
        val jsonGenerator: JsonGenerator = jsonFactory.createJsonGenerator(writer)
        jsonGenerator.useDefaultPrettyPrinter
        objectMapper.writeValue(jsonGenerator, record)
        writer.getBuffer.toString
      }
    }
    */
    val columns: List[Field] = MysqlUtils.getFieldDetails(dbName, tableName)
    val primarykeys: List[String] = MysqlUtils.getPrimarykeys(dbName, tableName)
    val namespace = "roadrunnr.cannedbeer.avrogen"
    val `type` = "record"
    val name = s"${dbName}_${tableName}"
    val doc = s"Auto-generated Avro schema for ${dbName}_$tableName "
    val headers = List(HeaderBuilder.getHeader("database", "string"),
      HeaderBuilder.getHeader("table", "string"),
      HeaderBuilder.getHeader("tableId", "long"),
      HeaderBuilder.getHeader("mutation", "string"))
    val fields = headers ::: columns

    val record = Table(namespace, `type`, name, fields)
    //println(record)

    val writer: StringWriter = new StringWriter
    val jsonGenerator: JsonGenerator = jsonFactory.createJsonGenerator(writer)
    jsonGenerator.useDefaultPrettyPrinter
    objectMapper.writeValue(jsonGenerator, record)
    writer.getBuffer.toString
    val schema = writer.getBuffer.toString

    //val filewriter: PrintWriter = new PrintWriter(s"${dbName}_${tableName}.avsc", "UTF-8")
    //filewriter.println(schema)
    //filewriter.close

    val fileName = s"${dbName}_${tableName}.avsc"
    val dir = new File("./avro-schemas")
    val actualFile = new File(dir, fileName)
    val output = new BufferedWriter(new FileWriter(actualFile))
    output.write(schema)
    output.close();

  }
}
