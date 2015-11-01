package mypipe.producer

import java.util.Properties
import com.typesafe.config.Config
import kafka.producer.KeyedMessage
import mypipe.api.event.AlterEvent
//import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
//import org.apache.kafka.clients.producer.{ KafkaProducer ⇒ KProducer, ProducerRecord }
import kafka.producer.{ Producer ⇒ KProducer, KeyedMessage, ProducerConfig }

/** Created by sumanth_bbn on 27/10/15.
 */
/** @param config:producer config  in pipe configuration from application.conf file
 */
class KafkaJsonProducer(config: Config) extends JsonProducer(config = config) {

  override val enablePrettyWriter = false
  protected val metadataBrokers = config.getString("metadata-brokers")

  val properties = new Properties()
  properties.put("request.required.acks", "1")
  properties.put("metadata.broker.list", metadataBrokers)
  properties.put("serializer.class", "kafka.serializer.StringEncoder");
  val conf = new ProducerConfig(properties)
  val producer = new KProducer[String, String](conf)
  //val producer = new KProducer[String, String](properties)

  //todo:use new kafka producer client,but facing the follwing error

  override def flush(): Boolean = {

    log.warn("flush is called in confluent producer")
    val iterator = QUEUE.iterator()
    iterator.hasNext match {
      case true ⇒ {
        val record = iterator.next()
        //val producerRecord: ProducerRecord[String, String] = new ProducerRecord[String, String](record._1, "key", record._2)
        //producer.send(producerRecord)
        val data = new KeyedMessage[String, String](record._1, "key", record._2)
        producer.send(data)
      }
      case false ⇒ log.warn("no more mess to flush")
    }
    //TODO:BUG,drain the Queue after flushing.DONE
    QUEUE.clear()

    true
  }

  override def handleAlter(event: AlterEvent): Boolean = {
    //TODO:should change this.
    log.info(s"\n$event\n")
    true
  }

}
