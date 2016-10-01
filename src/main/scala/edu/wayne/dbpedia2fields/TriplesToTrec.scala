package edu.wayne.dbpedia2fields

import edu.wayne.dbpedia2fields.Util.{extractFileName, extractLiteralText, splitTurtle}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{CoGroupedRDD, RDD}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Created by fsqcds on 6/24/16.
  */
object TriplesToTrec {
  val namePredicates = Seq(
    "<http://xmlns.com/foaf/0.1/name>",
    "<http://dbpedia.org/property/name>",
    "<http://xmlns.com/foaf/0.1/givenName>",
    "<http://xmlns.com/foaf/0.1/surname>",
    "<http://dbpedia.org/property/officialName>",
    "<http://dbpedia.org/property/fullname>",
    "<http://dbpedia.org/property/nativeName>",
    "<http://dbpedia.org/property/birthName>",
    "<http://dbpedia.org/ontology/birthName>",
    "<http://dbpedia.org/property/nickname>",
    "<http://dbpedia.org/property/showName>",
    "<http://dbpedia.org/property/shipName>",
    "<http://dbpedia.org/property/clubname>",
    "<http://dbpedia.org/property/unitName>",
    "<http://dbpedia.org/property/otherName>",
    "<http://dbpedia.org/ontology/formerName>",
    "<http://dbpedia.org/property/birthname>",
    "<http://dbpedia.org/property/alternativeNames>",
    "<http://dbpedia.org/property/otherNames>",
    "<http://dbpedia.org/property/names>",
    "<http://www.w3.org/2000/01/rdf-schema#label>"
  )

  val subjectPredicate = "<http://purl.org/dc/terms/subject>"

  val disambiguatesPredicate = "<http://dbpedia.org/ontology/wikiPageDisambiguates>"

  val redirectsPredicate = "<http://dbpedia.org/ontology/wikiPageRedirects>"

  val wikiLinkTextPredicate = "<http://dbpedia.org/ontology/wikiPageWikiLinkText>"

  def main(args: Array[String]): Unit = {
    val pathToTriples = args(0)
    val pathToOutput = args(1)

    val conf = new SparkConf().setAppName("TriplesToTrec")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val triples = sc.textFile(pathToTriples).filter { line =>
      line.startsWith("<")
    }.map(splitTurtle)

    val names = triples.filter { case (subj, pred, obj) =>
      namePredicates.contains(pred) && obj.startsWith("\"")
    }.map { case (subj, pred, obj) =>
      (subj, obj)
    }.distinct().cache()

    val categories = triples.filter { case (subj, pred, obj) =>
      pred == subjectPredicate
    }.map { case (subj, pred, obj) =>
      (obj, subj)
    }.join(names).map { case (obj, (subj, objName)) =>
      (subj, objName)
    }.distinct()

    val regularTriples = triples.filter { case (subj, pred, obj) =>
      !namePredicates.contains(pred) &&
        pred != subjectPredicate &&
        pred != disambiguatesPredicate &&
        pred != redirectsPredicate
    }

    val predNamesMap = sc.broadcast(regularTriples.map { case (subj, pred, obj) =>
      (pred, None)
    }.distinct().join(names).map { case (pred, (_, predName)) =>
      (pred, predName)
    }.distinct().collectAsMap())

    val filenames: RDD[(String, (Option[String], String))] = regularTriples.filter { case (subj, pred, obj) =>
      obj.startsWith("<http://dbpedia.org/resource/File:")
    }.map { case (subj, pred, obj) =>
      (subj, (None, extractFileName(obj)))
    }

    val attributes = regularTriples.filter { case (subj, pred, obj) =>
      obj.startsWith("\"")
    }.map { case (subj, pred, obj) =>
      (subj, (predNamesMap.value.get(pred), obj))
    }.union(filenames).distinct()

    val relatedEntityNames = regularTriples.filter { case (subj, pred, obj) =>
      obj.startsWith("<")
    }.map { case (subj, pred, obj) =>
      (obj, (predNamesMap.value.get(pred), subj))
    }.join(names).map { case (obj, ((predName, subj), objName)) =>
      (subj, (predName, objName))
    }.union(filenames).distinct()

    // Let's don't reverse predicate-object here. E.g for Animal_Farm author George_Orwell include "Animal Farm author"
    // into incoming links for George Orwell
    val incomingEntityNames = regularTriples.filter { case (subj, pred, obj) =>
      obj.startsWith("<")
    }.map { case (subj, pred, obj) =>
      (subj, (predNamesMap.value.get(pred), obj))
    }.join(names).map { case (subj, ((predName, obj), subjName)) =>
      (obj, (subjName, predName))
    }.distinct()

    // Consider only incoming disambiguates and redirects
    val similarEntityNames = triples.filter { case (subj, pred, obj) =>
      pred == disambiguatesPredicate || pred == redirectsPredicate
    }.map { case (subj, pred, obj) =>
      (subj, obj)
    }.join(names).map { case (subj, (obj, subjName)) =>
      (obj, subjName)
    }.union(
      // <dbo:wikiPageWikiLinkText>
      regularTriples.filter { case (subj, pred, obj) =>
        pred == wikiLinkTextPredicate
      }.map { case (subj, pred, obj) =>
        (subj, obj)
      }
    ).distinct()

    new CoGroupedRDD(Seq(names, attributes, categories, similarEntityNames, relatedEntityNames, incomingEntityNames),
      Partitioner.defaultPartitioner(names, attributes, categories, similarEntityNames, relatedEntityNames, incomingEntityNames)).
      mapValues { case Array(namesArray, attributesArray, categoriesArray, similarEntityNamesArray,
      relatedEntityNamesArray, incomingEntityNamesArray) =>
        (namesArray.asInstanceOf[Seq[String]],
          attributesArray.asInstanceOf[Seq[(Option[String], String)]],
          categoriesArray.asInstanceOf[Seq[String]],
          similarEntityNamesArray.asInstanceOf[Seq[String]],
          relatedEntityNamesArray.asInstanceOf[Seq[(Option[String], String)]],
          incomingEntityNamesArray.asInstanceOf[Seq[(String, Option[String])]])
      }.flatMap { case (entityUri, (namesSeq, attributesSeq, categoriesSeq, similarEntityNamesSeq,
    relatedEntityNamesSeq, incomingEntityNamesSeq)) =>
      Array("<DOC>\n<DOCNO>" + entityUri + "</DOCNO>\n<TEXT>") ++
        (if (namesSeq.nonEmpty) Array("<names>") ++
          namesSeq.map(extractLiteralText).distinct ++
          Array("</names>")
        else Seq()) ++
        (if (attributesSeq.nonEmpty) Array("<attributes>") ++
          attributesSeq.map {
            case (Some(predName), obj) => extractLiteralText(predName) + " " + extractLiteralText(obj)
            case (None, obj) => extractLiteralText(obj)
          }.distinct ++ Array("</attributes>")
        else Seq()) ++
        (if (categoriesSeq.nonEmpty) Array("<categories>") ++
          categoriesSeq.map(extractLiteralText).distinct ++
          Array("</categories>")
        else Seq()) ++
        (if (similarEntityNamesSeq.nonEmpty) Array("<similarentitynames>") ++
          similarEntityNamesSeq.map(extractLiteralText).distinct ++
          Array("</similarentitynames>")
        else Seq()) ++
        (if (relatedEntityNamesSeq.nonEmpty) Array("<relatedentitynames>") ++
          relatedEntityNamesSeq.map {
            case (Some(predName), objName) => extractLiteralText(predName) + " " + extractLiteralText(objName)
            case (None, objName) => extractLiteralText(objName)
          }.distinct ++
          Array("</relatedentitynames>")
        else Seq()) ++
        (if (incomingEntityNamesSeq.nonEmpty) Array("<incomingentitynames>") ++
          incomingEntityNamesSeq.map {
            case (subjName, Some(predName)) => extractLiteralText(subjName) + " " + extractLiteralText(predName)
            case (subjName, None) => extractLiteralText(subjName)
          }.distinct ++
          Array("</incomingentitynames>")
        else Seq()) ++
        Array("</TEXT>\n</DOC>")
    }.saveAsTextFile(pathToOutput)
  }
}
