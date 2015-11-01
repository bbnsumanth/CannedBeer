package mypipe.api

case class HostPortUserPass(val host: String, val port: Int, val user: String, val password: String)

/** singleton object'apply method will create HostPortUserPass obj
 */
object HostPortUserPass {

  def apply(hostPortUserPass: String) = {
    val params = hostPortUserPass.split(":")
    new HostPortUserPass(params(0), params(1).toInt, params(2), params(3))
  }
}