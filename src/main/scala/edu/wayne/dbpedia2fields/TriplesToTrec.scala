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

    val names = triples.filter { triple =>
      namePredicates.contains(triple._1)
    }.map { triple =>
      (triple._1, triple._3) // subj, obj
    }

    val categories = triples.filter { triple =>
      triple._2 == subjectPredicate
    }.map {
      triple => (triple._3, triple._1) // obj, subj
    }.join(names).map { pair => // obj, (subj, objName)
      (pair._2._1, pair._2._2) // subj, objName
    }

    val regularTriples = triples.filter { triple =>
      !namePredicates.contains(triple._2) &&
        triple._2 != subjectPredicate &&
        triple._2 != disambiguatesPredicate &&
        triple._3 != redirectsPredicate
    }

    val attributes = regularTriples.filter { triple =>
      triple._3.startsWith("\"")
    }.map { triple =>
      (triple._2, (triple._1, triple._3)) // pred, (subj, obj)
    }.leftOuterJoin(names).map { pair => // pred, ((subj, obj), predName)
      val subj = pair._2._1._1
      val obj = pair._2._1._2
      val predName = pair._2._2
      (subj, (predName, obj))
    }

    val relatedEntityNames = regularTriples.filter { triple =>
      triple._3.startsWith("<")
    }.map { triple =>
      (triple._2, (triple._1, triple._3)) // pred, (subj, obj)
    }.leftOuterJoin(names).map { pair => // pred, ((subj, obj), predName)
      val subj = pair._2._1._1
      val predName = pair._2._2
      val obj = pair._2._1._2
      (obj, (predName, subj))
    }.join(names).map { pair => // obj, ((predName, subj), objName)
      val subj = pair._2._1._2
      val predName = pair._2._1._1
      val objName = pair._2._2
      (subj, (predName, objName))
    }

    // Let's don't reverse predicate-object here. E.g for Animal_Farm author George_Orwell include "Animal Farm author"
    // into incoming links for George Orwell
    val incomingEntityNames = regularTriples.filter { triple =>
      triple._3.startsWith("<")
    }.map { triple =>
      (triple._2, (triple._1, triple._3)) // pred, (subj, obj)
    }.leftOuterJoin(names).map { pair => // pred, ((subj, obj), predName)
      val subj = pair._2._1._1
      val predName = pair._2._2
      val obj = pair._2._1._2
      (subj, (predName, obj))
    }.join(names).map { pair => // subj, ((predName, obj), subjName)
      val obj = pair._2._1._2
      val predName = pair._2._1._1
      val subjName = pair._2._2
      (obj, (subjName, predName))
    }

    // Consider only incoming disambiguates and redirects
    val similarEntityNames = triples.filter { triple =>
      triple._2 == disambiguatesPredicate || triple._2 == redirectsPredicate
    }.map {
      triple => (triple._1, triple._3) // subj, obj
    }.join(names).map { pair => // subj, (obj, subjName)
      (pair._2._1, pair._2._2) // obj, subjName
    }

    new CoGroupedRDD(Seq(names, attributes, categories, similarEntityNames, relatedEntityNames, incomingEntityNames),
      Partitioner.defaultPartitioner(names, attributes, categories, similarEntityNames, relatedEntityNames, incomingEntityNames))
      .flatMap { case (entityUri, names, attributes, categories, similarEntityNames, relatedEntityNames, incomingEntityNames) =>
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
