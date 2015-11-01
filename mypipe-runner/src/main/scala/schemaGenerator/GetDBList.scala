package schemaGenerator

import java.sql._
import java.util

/** Created by sumanth_bbn on 29/10/15.
 */
object GetDBList {

  def get(host: String, port: String, user: String, password: String) = {
    val results = new util.ArrayList[String]()
    val dbName = ""
    val url = s"jdbc:mysql://" + host + ":" + port + "/" + dbName + "?zeroDateTimeBehavior=convertToNull&user=" + user + "&password=" + password
    var con: Connection = null

    try {
      con = DriverManager.getConnection(url)

      val meta: DatabaseMetaData = con.getMetaData()
      val res: ResultSet = meta.getCatalogs()

      System.out.println("List of databases: ")

      while (res.next()) {
        val x = res.getString("TABLE_CAT")
        //System.out.println("   " + x);
        results.add(x)
      }
      res.close();
      con.close();
    } catch {
      case e: Exception ⇒ e.printStackTrace();
    }

    results

  }
}
