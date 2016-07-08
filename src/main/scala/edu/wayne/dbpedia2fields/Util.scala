package edu.wayne.dbpedia2fields

/**
  * Created by fsqcds on 7/7/16.
  */
object Util {
  def splitTurtle(str: String): (String, String, String) = {
    val splitStr = str.split(" ", 3)
    (splitStr(0), splitStr(1), splitStr(2).substring(0, splitStr(2).lastIndexOf(' ')))
  }
}
