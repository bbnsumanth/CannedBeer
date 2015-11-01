//import mypipe.util.Eval

//case class Person()
////val a = Class.forName("Person").asInstanceOf[Person]
val db ="db"
val table = "table"
/*
val mtype = "mtype"
s"${db}_${table}_${mtype}"

s"they are ${db}_$table "
val genericTopicFormat ="mypipe.kafka.generic-producer.topic-format"
val specificTopicFormat ="mypipe.kafka.specific-producer.topic-format"
val generictplFn = Eval[(String, String) ⇒ String]("{ (db: String, table: String) => { s\"" + genericTopicFormat + "\" } }")
*/

s"${db}_${table}"
