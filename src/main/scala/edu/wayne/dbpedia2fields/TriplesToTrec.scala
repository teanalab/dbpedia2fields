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

    val namesMap = sc.broadcast(names.collectAsMap())

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

    val attributes = regularTriples.filter { case (subj, pred, obj) =>
      obj.startsWith("\"")
    }.map { case (subj, pred, obj) =>
      (pred, (subj, obj))
    }.leftOuterJoin(names).map { case (pred, ((subj, obj), predName)) =>
      (subj, (predName, obj))
    }

    val relatedEntityNames = regularTriples.filter { case (subj, pred, obj) =>
      obj.startsWith("<")
    }.map { case (subj, pred, obj) =>
      (pred, (subj, obj))
    }.leftOuterJoin(names).map { case (pred, ((subj, obj), predName)) =>
      (obj, (predName, subj))
    }.join(names).map { case (obj, ((predName, subj), objName)) =>
      (subj, (predName, objName))
    }

    // Let's don't reverse predicate-object here. E.g for Animal_Farm author George_Orwell include "Animal Farm author"
    // into incoming links for George Orwell
    val incomingEntityNames = regularTriples.filter { case (subj, pred, obj) =>
      obj.startsWith("<")
    }.map { case (subj, pred, obj) =>
      (pred, (subj, obj))
    }.leftOuterJoin(names).map { case(pred, ((subj, obj), predName)) =>
      (subj, (predName, obj))
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
      mapValues { case Array(names, attributes, categories, similarEntityNames, relatedEntityNames, incomingEntityNames) =>
        (names.asInstanceOf[Array[String]],
          attributes.asInstanceOf[Array[(Option[String], String)]],
          categories.asInstanceOf[Array[String]],
          similarEntityNames.asInstanceOf[Array[String]],
          relatedEntityNames.asInstanceOf[Array[(Option[String], String)]],
          incomingEntityNames.asInstanceOf[Array[(String, Option[String])]])
      }.flatMap { case (entityUri, (names, attributes, categories, similarEntityNames, relatedEntityNames, incomingEntityNames)) =>
      Array("<DOC>\n<DOCNO>" + entityUri + "</DOCNO>\n<TEXT>") ++
        Array("<names>") ++
        names ++
        Array("</names>") ++
        Array("<attributes>") ++
        attributes ++
        Array("</attributes>") ++
        Array("<categories>") ++
        categories ++
        Array("</categories>") ++
        Array("<similarentitynames>") ++
        similarEntityNames ++
        Array("</similarentitynames>") ++
        Array("<relatedentitynames>") ++
        relatedEntityNames ++
        Array("</relatedentitynames>") ++
        Array("<incomingentitynames>") ++
        incomingEntityNames ++
        Array("</incomingentitynames>")
    }.saveAsTextFile(pathToOutput)
  }
}
