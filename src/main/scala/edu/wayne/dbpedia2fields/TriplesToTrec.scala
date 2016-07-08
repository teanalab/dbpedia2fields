package edu.wayne.dbpedia2fields

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.CoGroupedRDD
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

  def main(args: Array[String]): Unit = {
    val pathToTriples = args(0)
    val pathToOutput = args(1)

    val conf = new SparkConf().setAppName("TriplesToTrec")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val triples = sc.textFile(pathToTriples).filter { line =>
      line.startsWith("<")
    }.map(Util.splitTurtle)

    val names = triples.filter { case (subj, pred, obj) =>
      namePredicates.contains(pred)
    }.map { case (subj, pred, obj) =>
      (subj, obj)
    }

    val categories = triples.filter { case (subj, pred, obj) =>
      pred == subjectPredicate
    }.map { case (subj, pred, obj) =>
      (obj, subj)
    }.join(names).map { case (obj, (subj, objName)) =>
      (subj, objName)
    }

    val regularTriples = triples.filter { case (subj, pred, obj) =>
      !namePredicates.contains(pred) &&
        pred != subjectPredicate &&
        pred != disambiguatesPredicate &&
        pred != redirectsPredicate
    }

    val predNamesMap = sc.broadcast(regularTriples.map { case (subj, pred, obj) =>
      (pred, None)
    }.join(names).map { case (pred, (_, predName)) =>
      (pred, predName)
    }.collectAsMap())

    val attributes = regularTriples.filter { case (subj, pred, obj) =>
      obj.startsWith("\"")
    }.map { case (subj, pred, obj) =>
      (subj, (predNamesMap.value.get(pred), obj))
    }

    val relatedEntityNames = regularTriples.filter { case (subj, pred, obj) =>
      obj.startsWith("<")
    }.map { case (subj, pred, obj) =>
      (obj, (predNamesMap.value.get(pred), subj))
    }.join(names).map { case (obj, ((predName, subj), objName)) =>
      (subj, (predName, objName))
    }

    // Let's don't reverse predicate-object here. E.g for Animal_Farm author George_Orwell include "Animal Farm author"
    // into incoming links for George Orwell
    val incomingEntityNames = regularTriples.filter { case (subj, pred, obj) =>
      obj.startsWith("<")
    }.map { case (subj, pred, obj) =>
      (subj, (predNamesMap.value.get(pred), obj))
    }.join(names).map { case (subj, ((predName, obj), subjName)) =>
      (obj, (subjName, predName))
    }

    // Consider only incoming disambiguates and redirects
    val similarEntityNames = triples.filter { case (subj, pred, obj) =>
      pred == disambiguatesPredicate || pred == redirectsPredicate
    }.map { case (subj, pred, obj) =>
      (subj, obj)
    }.join(names).map { case (subj, (obj, subjName)) =>
      (obj, subjName)
    }

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
        Array("<names>") ++
        namesSeq ++
        Array("</names>") ++
        Array("<attributes>") ++
        attributesSeq ++
        Array("</attributes>") ++
        Array("<categories>") ++
        categoriesSeq ++
        Array("</categories>") ++
        Array("<similarentitynames>") ++
        similarEntityNamesSeq ++
        Array("</similarentitynames>") ++
        Array("<relatedentitynames>") ++
        relatedEntityNamesSeq ++
        Array("</relatedentitynames>") ++
        Array("<incomingentitynames>") ++
        incomingEntityNamesSeq ++
        Array("</incomingentitynames>")
    }.saveAsTextFile(pathToOutput)
  }
}
