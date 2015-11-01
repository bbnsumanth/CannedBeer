package mypipe.producer

import java.io.{ Serializable, StringWriter }
import java.util.concurrent.LinkedBlockingQueue
import com.fasterxml.jackson.core.{ JsonGenerator, JsonFactory }
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.Config
import mypipe.api.event.{ UpdateMutation, SingleValuedMutation, Mutation, AlterEvent }
import mypipe.api.producer.Producer
import org.slf4j.LoggerFactory

/** Created by sumanth_bbn on 30/10/15.
 */
case class JsonEvent(database: String, table: String, mutation: String, columns: List[Field])

case class Field(columnName: String, columnType: String, value: java.io.Serializable, isPrimary: Boolean)

abstract class JsonProducer(config: Config) extends Producer(config = config) {
  /** alterTable and flush should be implementedd by child classes:KafkaJsonProducer,stdoutJsonProducer
   */

  protected val log = LoggerFactory.getLogger(getClass)
  //TODO:change the type
  val QUEUE = new LinkedBlockingQueue[(String, String)]()
  //JSON parsers
  val objectMapper: ObjectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  val jsonFactory: JsonFactory = new JsonFactory()
  val enablePrettyWriter = true

  //TODO: convert data ROW to JSON data,trick is on how to detect types for columns
  //TODO: send the JSON records as messages to kafka.which kafka producer to use ??
  /** JSON MESSAGE FORMAT
   *
   *  {
   *  "database" : "kafkatest",
   *  "table" : "kafka",
   *  "mutation" : "insert",
   *  "columns" : [ {
   *  "columnName" : "col1",
   *  "columnType" : "varchar",
   *  "value" : "arpit",
   *  "isPrimary" : false
   *  }, {
   *  "columnName" : "col2",
   *  "columnType" : "varchar",
   *  "value" : "give me some weed",
   *  "isPrimary" : false
   *  } ]
   *  }
   */
  def getKafkaTopic(mutation: Mutation): String = {
    val (db, table) = (mutation.table.db, mutation.table.name)
    s"${db}_${table}"
  }

  def insertOrDeleteMutationToAvro(mutation: SingleValuedMutation): List[(String, JsonEvent)] = {
    val db = mutation.table.db
    val table = mutation.table.name
    val mutationType = Mutation.typeAsString(mutation)
    val topic = getKafkaTopic(mutation)
    mutation.rows.map(row ⇒ {
      val fields = row.columns.map(col ⇒ {
        val columnName = col._1
        val columnType = col._2.metadata.colType.str
        //todo revert back to toString
        val value = col._2.value
        val isPrimary = col._2.metadata.isPrimaryKey
        Field(columnName, columnType, value, isPrimary)
      }).toList
      (topic, JsonEvent(db, table, mutationType, fields))
    })
  }

  def updateMutationToAvro(mutation: UpdateMutation): List[(String, JsonEvent)] = {
    val db = mutation.table.db
    val table = mutation.table.name
    val mutationType = Mutation.typeAsString(mutation)
    val topic = getKafkaTopic(mutation)
    mutation.rows.map(row ⇒ {
      val fields = row._2.columns.map(col ⇒ {
        val columnName = col._1
        val columnType = col._2.metadata.colType.str
        //todo:check whether leaving the value as serializable won't be a problem
        val value = col._2.value
        val isPrimary = col._2.metadata.isPrimaryKey
        Field(columnName, columnType, value, isPrimary)
      }).toList
      (topic, JsonEvent(db, table, mutationType, fields))
    })
  }

  def getJsonRecord(mutation: Mutation): List[(String, JsonEvent)] = {
    Mutation.getMagicByte(mutation) match {
      case Mutation.InsertByte ⇒ insertOrDeleteMutationToAvro(mutation.asInstanceOf[SingleValuedMutation])
      case Mutation.UpdateByte ⇒ updateMutationToAvro(mutation.asInstanceOf[UpdateMutation])
      case Mutation.DeleteByte ⇒ insertOrDeleteMutationToAvro(mutation.asInstanceOf[SingleValuedMutation])
      case _ ⇒ {
        log.error(s"Unexpected mutation type ${mutation.getClass} encountered; retuning empty empty Json record")
        val topic = getKafkaTopic(mutation)
        List((topic, JsonEvent("", "", "", List(Field("", "", "", false)))))

      }

    }
  }

  //TODO:decide on the format for message ,if possible include metadata like column type.
  //handle mutation and ccreate json records,then convert them into kafka message,add them to Queue
  override def queue(mutation: Mutation): Boolean = {
    try {

      val writer: StringWriter = new StringWriter
      val jsonGenerator: JsonGenerator = jsonFactory.createJsonGenerator(writer)

      enablePrettyWriter match {
        case true  ⇒ jsonGenerator.useDefaultPrettyPrinter
        case false ⇒
      }

      val records = getJsonRecord(mutation)
      records.foreach(record ⇒ {

        objectMapper.writeValue(jsonGenerator, record._2)
        val message = writer.getBuffer.toString
        log.info(s"message added to queue")
        QUEUE.add((record._1, message))

      })
      true
    } catch {
      case e: Exception ⇒ log.error(s"failed to queue: ${e.getMessage}\n${e.getStackTraceString}"); false
    }
  }

  override def queueList(mutations: List[Mutation]): Boolean = {
    try {
      val writer: StringWriter = new StringWriter
      val jsonGenerator: JsonGenerator = jsonFactory.createJsonGenerator(writer)
      enablePrettyWriter match {
        case true  ⇒ jsonGenerator.useDefaultPrettyPrinter
        case false ⇒
      }
      mutations.foreach(mutation ⇒ {
        val records = getJsonRecord(mutation)
        records.foreach(record ⇒ {
          objectMapper.writeValue(jsonGenerator, record._2)
          val message = writer.getBuffer.toString
          log.info(s"message added to queue")
          QUEUE.add((record._1, message))
        })
      })
      true
    } catch {
      case e: Exception ⇒ log.error(s"failed to queue: ${e.getMessage}\n${e.getStackTraceString}"); false
    }
  }

}
