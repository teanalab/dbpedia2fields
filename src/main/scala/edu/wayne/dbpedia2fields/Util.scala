package edu.wayne.dbpedia2fields

/**
  * Created by fsqcds on 7/7/16.
  */
object Util {
  def splitTurtle(str: String): (String, String, String) = {
    try {
      val splitStr = str.split(" ", 3)
      val objEnd = if (splitStr(2).lastIndexOf(' ') > -1) splitStr(2).lastIndexOf(' ') else splitStr(2).lastIndexOf('.')
      (splitStr(0), splitStr(1), splitStr(2).substring(0, objEnd))
    } catch {
      case e: Exception =>
        println(str)
        throw new Exception(str, e)
    }
  }

  // since we only have english string literals
  def extractLiteralText(obj: String): String = {
    StringContext treatEscapes obj.substring(obj.indexOf("\"") + 1, obj.lastIndexOf("\""))
  }
}
