package mypipe.producer

import java.io.File
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue
import java.util.logging.Logger

import com.typesafe.config.Config
import mypipe.api.event.{ AlterEvent, Mutation, SingleValuedMutation, UpdateMutation }
import mypipe.api.producer.Producer
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericData, GenericRecord }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
import org.slf4j.LoggerFactory

/** Created by sumanth_bbn on 27/10/15.
 */
/** @param config:producer config  in pipe configuration from application.conf file
 */
class ConfluetProducer(config: Config) extends Producer(config = config) {

  val log = Logger.getLogger(getClass.getName)
  protected val logger = LoggerFactory.getLogger(getClass)
  val metadataBrokers = config.getString("metadata-brokers")
  val schemaRegistryUrl = config.getString("schema-repo-client")
  //to find .avsc files for all tables and databases
  val schemaFilesPath = config.getString("avro-schemas-path")

  val props = new Properties()
  props.put("request.required.acks", "1")
  props.put("bootstrap.servers", metadataBrokers) //diff from confluent example props.put("bootstrap.servers", "localhost:9092")
  props.put("schema.registry.url", schemaRegistryUrl)
  props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
  props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
  //new kafka jvm producer
  val producer = new KafkaProducer[String, GenericRecord](props)
  //queue to store producer records
  val QUEUE = new LinkedBlockingQueue[ProducerRecord[String, GenericRecord]]()

  //def getKafkaTopic(mutation: Mutation): String = KafkaUtil.confluentTopic(mutation)
  def getKafkaTopic(mutation: Mutation): String = {
    val (db, table) = (mutation.table.db, mutation.table.name)
    s"${db}_${table}"
  }

  def getSchema(mutation: Mutation) = {
    logger.warn("getSchema called")
    val name = getKafkaTopic(mutation)
    //TODO:gracefully handle exception here for java.io.FileNotFoundException:
    //to
    //Try(new Schema.Parser().parse(new File(s"${schemaFilesPath}/${name}.avsc")))
    val schema = new Schema.Parser().parse(new File(s"${schemaFilesPath}/${name}.avsc"))
    println(schema.toString(true))
    schema
  }

  /** Adds a header into the given Record based on the Mutation's
   *  database, table, and tableId.
   *  @param record
   *  @param mutation
   */
  protected def header(record: GenericData.Record, mutation: Mutation) {
    record.put("database", mutation.table.db)
    record.put("table", mutation.table.name)
    record.put("tableId", mutation.table.id)
    record.put("mutation", Mutation.typeAsString(mutation))
    /*
    // TODO: avoid null check
    if (mutation.txid != null && record.getSchema().getField("txid") != null) {
      val uuidBytes = ByteBuffer.wrap(new Array[Byte](16))
      uuidBytes.putLong(mutation.txid.getMostSignificantBits)
      uuidBytes.putLong(mutation.txid.getLeastSignificantBits)
      record.put("txid", new GenericData.Fixed(Guid.getClassSchema, uuidBytes.array))
    }
    */
  }

  //TODO:look at how to describe schema for different mutation type for same db_table(topic),
  //TODO:schema registry only stores two schemas for a topic,one for key and one for value.
  //TODO:one possibility is to use same schema for all mutations ,but include mutation type as the field.

  def getAvroRecord(mutation: Mutation, schema: Schema): List[ProducerRecord[String, GenericRecord]] = {
    //crete an producer record from mutation
    Mutation.getMagicByte(mutation) match {
      case Mutation.InsertByte ⇒ insertOrDeleteMutationToAvro(mutation.asInstanceOf[SingleValuedMutation], schema)
      case Mutation.UpdateByte ⇒ updateMutationToAvro(mutation.asInstanceOf[UpdateMutation], schema)
      case Mutation.DeleteByte ⇒ insertOrDeleteMutationToAvro(mutation.asInstanceOf[SingleValuedMutation], schema)
      case _ ⇒
        logger.error(s"Unexpected mutation type ${mutation.getClass} encountered; retuning empty Avro GenericData.Record(schema=$schema")
        List(new GenericData.Record(schema)).map(record ⇒ {
          val topicName = getKafkaTopic(mutation)
          //TODO:change key,use primary key in the table as key,so that we can use log compaction of kafka
          val producerRecord: ProducerRecord[String, GenericRecord] = new ProducerRecord[String, GenericRecord](topicName, "key", record)
          producerRecord
        })
    }

  }

  protected def insertOrDeleteMutationToAvro(mutation: SingleValuedMutation, schema: Schema): List[ProducerRecord[String, GenericRecord]] = {

    mutation.rows.map(row ⇒ {
      val record = new GenericData.Record(schema)
      row.columns.foreach(col ⇒ Option(schema.getField(col._1)).foreach(f ⇒ record.put(f.name(), col._2.value)))
      //TODO:temp change,should revert
      header(record, mutation)
      val topicName = getKafkaTopic(mutation)
      //TODO:change key,use primary key in the table as key,so that we can use log compaction of kafka
      val producerRecord: ProducerRecord[String, GenericRecord] = new ProducerRecord[String, GenericRecord](topicName, "key", record)
      logger.warn("avro recored generated")
      producerRecord

    })
  }

  protected def updateMutationToAvro(mutation: UpdateMutation, schema: Schema): List[ProducerRecord[String, GenericRecord]] = {

    mutation.rows.map(row ⇒ {
      val record = new GenericData.Record(schema)
      //row._1.columns.foreach(col ⇒ Option(schema.getField("old_" + col._1)).foreach(f ⇒ record.put(f.name(), col._2.value)))
      //row._2.columns.foreach(col ⇒ Option(schema.getField("new_" + col._1)).foreach(f ⇒ record.put(f.name(), col._2.value)))
      //TODO:using only one schema for any type of  mutations,so incase of update we are only storing new values.
      row._2.columns.foreach(col ⇒ Option(schema.getField(col._1)).foreach(f ⇒ record.put(f.name(), col._2.value)))
      header(record, mutation)
      val topicName = getKafkaTopic(mutation)
      //TODO:change key,use primary key in the table as key,so that we can use log compaction of kafka
      val producerRecord: ProducerRecord[String, GenericRecord] = new ProducerRecord[String, GenericRecord](topicName, "key", record)
      producerRecord

    })
  }

  override def queue(mutation: Mutation): Boolean = {
    try {
      val schema = getSchema(mutation)
      val records = getAvroRecord(mutation, schema)
      records.foreach(record ⇒ {
        logger.warn("record added to Queue")
        QUEUE.add(record)
      })
      true
    } catch {
      case e: Exception ⇒ logger.error(s"failed to queue: ${e.getMessage}\n${e.getStackTraceString}"); false
    }
  }

  override def queueList(mutations: List[Mutation]): Boolean = {
    try {
      mutations.foreach(mutation ⇒ {
        val schema = getSchema(mutation)
        val records = getAvroRecord(mutation, schema)
        records.foreach(record ⇒ {
          QUEUE.add(record)
        })
      })
      true
    } catch {
      case e: Exception ⇒ logger.error(s"failed to queue: ${e.getMessage}\n${e.getStackTraceString}"); false
    }

  }

  override def flush(): Boolean = {
    log.warning("flush is called in confluent producer")
    val iterator = QUEUE.iterator()
    iterator.hasNext match {
      case true  ⇒ producer.send(iterator.next())
      case false ⇒ log.warning("no more mess to flush")
    }
    //TODO:BUG,drain the Queue afrter flushing.DONE
    QUEUE.clear()
    true
  }

  override def handleAlter(event: AlterEvent): Boolean = {
    //TODO:should implement this
    true
  }

}
