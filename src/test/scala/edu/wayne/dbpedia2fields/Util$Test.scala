package edu.wayne.dbpedia2fields

import org.scalatest.FunSuite

/**
  * Created by fsqcds on 7/7/16.
  */
class Util$Test extends FunSuite {

  test("testSplitTurte") {
    val tripleOne = Util.splitTurtle("<http://dbpedia.org/resource/Anarchism> <http://purl.org/dc/terms/subject> <http://dbpedia.org/resource/Category:Political_culture> .")
    assert(tripleOne._1 === "<http://dbpedia.org/resource/Anarchism>")
    assert(tripleOne._2 === "<http://purl.org/dc/terms/subject>")
    assert(tripleOne._3 === "<http://dbpedia.org/resource/Category:Political_culture>")

    val tripleTwo = Util.splitTurtle("<http://dbpedia.org/resource/Category:Professional_wrestling> <http://www.w3.org/2000/01/rdf-schema#label> \"Professional wrestling\"@en .")
    assert(tripleTwo._1 === "<http://dbpedia.org/resource/Category:Professional_wrestling>")
    assert(tripleTwo._2 === "<http://www.w3.org/2000/01/rdf-schema#label>")
    assert(tripleTwo._3 === "\"Professional wrestling\"@en")

    val tripleThree = Util.splitTurtle("<http://dbpedia.org/resource/Academy_Award_for_Best_Production_Design> <http://dbpedia.org/ontology/abstract> \"The Academy Awards are the oldest awards ceremony for achievements in motion pictures. The Academy Award for Best Production Design recognizes achievement in art direction on a film. The category's original name was Best Art Direction, but was changed to its current name in 2012 for the 85th Academy Awards.  This change resulted from the Art Director's branch of the Academy being renamed the Designer's branch. Since 1947, the award is shared with the Set decorator(s).The films below are listed with their production year (for example, the 2000 Academy Award for Best Art Direction is given to a film from 1999). In the lists below, the winner of the award for each year is shown first, followed by the other nominees.\"@en .")
    assert(tripleThree._1 === "<http://dbpedia.org/resource/Academy_Award_for_Best_Production_Design>")
    assert(tripleThree._2 === "<http://dbpedia.org/ontology/abstract>")
    assert(tripleThree._3 === "\"The Academy Awards are the oldest awards ceremony for achievements in motion pictures. The Academy Award for Best Production Design recognizes achievement in art direction on a film. The category's original name was Best Art Direction, but was changed to its current name in 2012 for the 85th Academy Awards.  This change resulted from the Art Director's branch of the Academy being renamed the Designer's branch. Since 1947, the award is shared with the Set decorator(s).The films below are listed with their production year (for example, the 2000 Academy Award for Best Art Direction is given to a film from 1999). In the lists below, the winner of the award for each year is shown first, followed by the other nominees.\"@en")

    // actual error in data
    val tripleFour = Util.splitTurtle("<http://dbpedia.org/resource/Lincoln_M._Alexander_Parkway> <http://dbpedia.org/ontology/routeStartDirection> \"West\".")
    assert(tripleFour._1 === "<http://dbpedia.org/resource/Lincoln_M._Alexander_Parkway>")
    assert(tripleFour._2 === "<http://dbpedia.org/ontology/routeStartDirection>")
    assert(tripleFour._3 === "\"West\"")
  }

  test("extractLiteralText") {
    assert(Util.extractLiteralText("\"Professional wrestling\"@en") === "Professional wrestling")
    assert(Util.extractLiteralText("\"An American in Paris is a jazz-influenced symphonic poem by the American composer George Gershwin, written in 1928. Inspired by the time Gershwin had spent in Paris, it evokes the sights and energy of the French capital in the 1920s and is one of his best-known compositions.Gershwin composed An American in Paris on commission from the conductor Walter Damrosch. He scored the piece for the standard instruments of the symphony orchestra plus celesta, saxophones, and automobile horns. He brought back some Parisian taxi horns for the New York premiere of the composition, which took place on December 13, 1928 in Carnegie Hall, with Damrosch conducting the New York Philharmonic. Gershwin completed the orchestration on November 18, less than four weeks before the work's premiere.Gershwin collaborated on the original program notes with the critic and composer Deems Taylor, noting that: \\\"My purpose here is to portray the impression of an American visitor in Paris as he strolls about the city and listens to various street noises and absorbs the French atmosphere.\\\" When the tone poem moves into the blues, \\\"our American friend ... has succumbed to a spasm of homesickness.\\\" But, \\\"nostalgia is not a fatal disease.\\\" The American visitor \\\"once again is an alert spectator of Parisian life\\\" and \\\"the street noises and French atmosphere are triumphant.\\\"\"@en") === "An American in Paris is a jazz-influenced symphonic poem by the American composer George Gershwin, written in 1928. Inspired by the time Gershwin had spent in Paris, it evokes the sights and energy of the French capital in the 1920s and is one of his best-known compositions.Gershwin composed An American in Paris on commission from the conductor Walter Damrosch. He scored the piece for the standard instruments of the symphony orchestra plus celesta, saxophones, and automobile horns. He brought back some Parisian taxi horns for the New York premiere of the composition, which took place on December 13, 1928 in Carnegie Hall, with Damrosch conducting the New York Philharmonic. Gershwin completed the orchestration on November 18, less than four weeks before the work's premiere.Gershwin collaborated on the original program notes with the critic and composer Deems Taylor, noting that: \"My purpose here is to portray the impression of an American visitor in Paris as he strolls about the city and listens to various street noises and absorbs the French atmosphere.\" When the tone poem moves into the blues, \"our American friend ... has succumbed to a spasm of homesickness.\" But, \"nostalgia is not a fatal disease.\" The American visitor \"once again is an alert spectator of Parisian life\" and \"the street noises and French atmosphere are triumphant.\"")
    assert(Util.extractLiteralText("\"209850\"^^<http://www.w3.org/2001/XMLSchema#integer>") === "209850")
    assert(Util.extractLiteralText("\"ArithmeticMean\"@en") == "Arithmetic Mean")

  }

  test("extractFileName") {
    assert(Util.extractLiteralText(Util.extractFileName("<http://dbpedia.org/resource/File:Public_Health_Service_Achievement_Medal_ribbon.png>")) === "Public Health Service Achievement Medal ribbon")
    assert(Util.extractLiteralText(Util.extractFileName("<http://dbpedia.org/resource/File:HavredeGrace.jpg>")) == "Havrede Grace")
  }
}
