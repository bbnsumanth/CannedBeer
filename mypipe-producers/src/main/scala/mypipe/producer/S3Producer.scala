package mypipe.producer.stdout

import com.typesafe.config.Config
import mypipe.api.event.{ Mutation, AlterEvent }
import mypipe.api.producer.Producer

/** Created by sumanth_bbn on 30/10/15.
 */
class S3Producer(config: Config) extends Producer(config) {
  override def queue(mutation: Mutation): Boolean = ???

  override def flush(): Boolean = ???

  override def queueList(mutation: List[Mutation]): Boolean = ???

  override def handleAlter(event: AlterEvent): Boolean = ???
}
