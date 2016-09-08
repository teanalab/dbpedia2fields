package edu.wayne.dbpedia2fields

import org.apache.commons.lang3.StringEscapeUtils

/**
  * Created by fsqcds on 7/7/16.
  */
object Util {
  def splitTurtle(str: String): (String, String, String) = {
    try {
      val splitStr = str.split(" ", 3)
      (splitStr(0).trim, splitStr(1).trim, splitStr(2).substring(0, splitStr(2).lastIndexOf('.')).trim)
    } catch {
      case e: Exception =>
        throw new Exception(str, e)
    }
  }

  // since we only have english string literals
  def extractLiteralText(obj: String): String = {
    try {
      StringEscapeUtils.unescapeJava(obj.substring(obj.indexOf("\"") + 1, obj.lastIndexOf("\"")))
    } catch {
      case e: Exception =>
        throw new Exception(obj, e)
    }
  }

  def extractFileName(obj: String): String = {
    '"' + obj.stripPrefix("<http://dbpedia.org/resource/File:").stripSuffix(">").replaceAll("""\.[a-zA-Z]*$""", "")
      .replace('_', ' ') + '"'
  }
}
