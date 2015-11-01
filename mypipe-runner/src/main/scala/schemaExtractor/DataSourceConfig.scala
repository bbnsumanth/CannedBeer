package schemaExtractor

/** Created by sumanth_bbn on 28/10/15.
 */
/** @param dbName db name
 *  @param host server ip
 *  @param port server port
 *  @param user username for server
 *  @param password password for server
 *  @param minPoolSize min pool size
 *  @param maxPoolSize max pool size
 */
case class DataSourceConfig(
    dbName: String,
    host: String,
    port: String = "3306",
    user: String,
    password: String,
    minPoolSize: Int = 1,
    maxPoolSize: Int = 5) {
  /** connection url for a db
   */
  def getJDBCUrl: String = {
    val connectionUrl: String = s"jdbc:mysql://" + host + ":" + port + "/" + dbName + "?zeroDateTimeBehavior=convertToNull&user=" + user + "&password=" + password
    connectionUrl
  }

}
