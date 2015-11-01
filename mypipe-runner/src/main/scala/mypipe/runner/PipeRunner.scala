package mypipe.runner

import mypipe.api.producer.Producer
import mypipe.mysql.{ MySQLBinaryLogConsumer, BinaryLogFilePosition }
import mypipe.pipe.Pipe
import schemaExtractor.DataSourceConfig
import schemaGenerator.{ Generator, GetDBList }

import scala.collection.JavaConverters._
import mypipe.api.{ Conf, HostPortUserPass }
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.collection.immutable.Iterable

object PipeRunner extends App {

  import PipeRunnerUtil._

  protected val log = LoggerFactory.getLogger(getClass)
  protected val conf = ConfigFactory.load()

  lazy val producers: Map[String, Class[Producer]] = loadProducerClasses(conf, "mypipe.producers")
  lazy val consumers: Map[String, HostPortUserPass] = loadConsumerConfigs(conf, "mypipe.consumers")
  lazy val pipes: Seq[Pipe] = createPipes(conf, "mypipe.pipes", producers, consumers)

  val enable_avrogen = conf.getBoolean("mypipe.enable-avro-gen")
  enable_avrogen match {
    case false ⇒ log.info("avro gen is disabled")
    case true  ⇒ runAvroGen(consumers)
  }

  if (pipes.isEmpty) {
    log.info("No pipes defined, exiting.")
    sys.exit()
  }

  sys.addShutdownHook({
    log.info("Shutting down...")
    pipes.foreach(p ⇒ p.disconnect())
  })

  log.info(s"Connecting ${pipes.size} pipes...")
  pipes.foreach(_.connect())
}

object PipeRunnerUtil {

  protected val log = LoggerFactory.getLogger(getClass)

  def runAvroGen(consumers: Map[String, HostPortUserPass]): Unit = {

    consumers.foreach(x ⇒ {
      val host = x._2.host
      val port = x._2.port.toString
      val user = x._2.user
      val password = x._2.password
      import scala.collection.JavaConversions._

      val dbList = GetDBList.get(host, port, user, password).asScala.toList

      val configList = dbList.map(db ⇒ {
        DataSourceConfig(db, host, port, user, password)
      })

      val Generator = new Generator(configList)

      dbList.foreach(db ⇒ {
        Generator.generateSchemaForAllTablesInDB(db)
      })

    })

  }

  def loadProducerClasses(conf: Config, key: String): Map[String, Class[Producer]] = {
    //Conf.loadClassesForKey[Producer](key)
    //TODO:Change the implementation from Conf.conf because it represents reference.conf in resources.
    //TODO:STATUS:DONE
    val PRODUCERS = conf.getObject(key).asScala

    val result = PRODUCERS.map(kv ⇒ {
      val name = kv._1
      val classConf = conf.getConfig(s"$key.$name")
      val clazz = classConf.getString("class")
      (name, Class.forName(clazz).asInstanceOf[Class[Producer]])
    }).toMap
    log.warn(s"producers:::::$result")
    result

  }

  def loadConsumerConfigs(conf: Config, key: String): Map[String, HostPortUserPass] = {
    val CONSUMERS = conf.getObject(key).asScala

    CONSUMERS.map(kv ⇒ {
      val name = kv._1
      val consConf = conf.getConfig(s"$key.$name")
      val source = consConf.getString("source")
      val params = HostPortUserPass(source)
      (name, params)
    }).toMap
  }

  def createPipes(conf: Config,
                  key: String,
                  producerClasses: Map[String, Class[Producer]],
                  consumerConfigs: Map[String, HostPortUserPass]): Seq[Pipe] = {

    val PIPES = conf.getObject(key).asScala
    //[pipiename-pipe obj]

    PIPES.map(kv ⇒ {

      val pipeName = kv._1

      log.info(s"Loading configuration for $pipeName pipe")

      val pipeConf = conf.getConfig(s"$key.$pipeName") //we get particular pipe
      val enabled = pipeConf.hasPath("enabled") match {
        case true  ⇒ pipeConf.getBoolean("enabled")
        case false ⇒ true
      }
      enabled match {
        case true ⇒ {

          val consumersMap = pipeConf.getObject("consumers").asScala
          val consumerInstances = consumersMap.map(kv ⇒ {
            val consumerName = kv._1
            val consumerConfig = pipeConf.getConfig(s"consumers.$consumerName")

            val isCustom = consumerConfig.hasPath("customBinLog") match {
              case true  ⇒ consumerConfig.getBoolean("customBinLog")
              case false ⇒ false
            }

            val (binlogFileName, binlogPosition) = consumerConfig.hasPath("binlogFileName") && consumerConfig.hasPath("binLogPosition") match {
              case true  ⇒ (consumerConfig.getString("binlogFileName"), consumerConfig.getLong("binLogPosition"))
              case false ⇒ ("", 0.toLong) //this gives current BinaryLogFilePosition
            }
            log.warn(s"isCustom:$isCustom , binlogFileName:$binlogFileName , binlogPosition:$binlogPosition for $consumerName in pipe $pipeName")

            isCustom match {
              case true ⇒ {
                val consumer = createConsumer(pipeName, consumerConfigs(consumerName))
                //set custom binlogposition
                val binlogFileAndPos = new BinaryLogFilePosition(binlogFileName, binlogPosition)
                log.warn(s"$binlogFileAndPos for $consumerName in pipe $pipeName ")
                //set the binlogFileAndPosition to consumer obj
                consumer.setBinaryLogPosition(binlogFileAndPos)
                consumer

              }
              case false ⇒ {
                val consumer = createConsumer(pipeName, consumerConfigs(consumerName))
                //get binlogFileAndPos from config
                //if no file in tmp/mypipe directory for the pipename-host-port.poss,None is returned from Conf.binlogLoadFilePosition
                val binlogFileAndPos = Conf.binlogLoadFilePosition(consumer.id, pipeName = pipeName).getOrElse(BinaryLogFilePosition.current)
                //set the binlogFileAndPosition to consumer obj
                consumer.setBinaryLogPosition(binlogFileAndPos)
                consumer
              }
            }

          }).toList

          // the following hack assumes a single producer per pipe
          // since we don't support multiple producers correctly when
          // tracking offsets (we'll track offsets for the entire
          // pipe and not per producer
          val producers = pipeConf.getObject("producer")
          //log.warn(s"producers:$producers")
          val producerName = producers.entrySet().asScala.head.getKey
          //log.warn(s"producerName:$producerName")
          val producerConfig = pipeConf.getConfig(s"producer.$producerName")
          //log.warn(s"producer config $producerConfig")
          log.warn(s"producer class : ${producerClasses(producerName)}")
          val producerInstance = createProducer(producerName, producerConfig, producerClasses(producerName))
          log.info(s"connecting pipe for the producer $producerInstance")
          new Pipe(pipeName, consumerInstances, producerInstance)

        }
        case false ⇒ {
          null
        }
      }

    }).filter(_ != null).toSeq
  }

  protected def createConsumer(pipeName: String, params: HostPortUserPass): MySQLBinaryLogConsumer = {
    MySQLBinaryLogConsumer(params.host, params.port, params.user, params.password)
  }

  protected def createProducer(id: String, config: Config, clazz: Class[Producer]): Producer = {
    try {
      val ctor = clazz.getConstructor(classOf[Config])
      log.warn(s"constructor for producer = $ctor")

      if (ctor == null) throw new NullPointerException(s"Could not load ctor for class $clazz, aborting.")

      val producer = ctor.newInstance(config)
      producer
    } catch {
      case e: Exception ⇒
        log.error(s"Failed to configure producer $id: ${e.getMessage}\n${e.getStackTrace.mkString("\n")}")
        null
    }
  }
}
