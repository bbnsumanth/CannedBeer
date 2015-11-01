package mypipe.producer.stdout

import com.typesafe.config.Config
import mypipe.api.event.AlterEvent
import mypipe.producer.JsonProducer

/** Created by sumanth_bbn on 31/10/15.
 */
class StdoutJsonProducer(config: Config) extends JsonProducer(config = config) {
  override val enablePrettyWriter = true

  override def flush(): Boolean = {
    //log.warn("flush is called in json producer ")
    val iterator = QUEUE.iterator()
    iterator.hasNext match {

      case true ⇒ {
        val message = iterator.next()
        log.info(s"${message._2}")
      }
      case false ⇒ log.warn("no more mess to flush")
    }
    //TODO:BUG,drain the Queue afrter flushing.DONE
    QUEUE.clear()
    true
  }

  override def handleAlter(event: AlterEvent): Boolean = {
    //TODO:should change this.
    log.info(s"\n$event\n")
    true
  }
}
