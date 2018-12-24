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
    "<http://dbpedia.org/property/showName>",
    "<http://dbpedia.org/property/shipName>",
    "<http://dbpedia.org/property/clubname>",
    "<http://dbpedia.org/property/unitName>",
    "<http://dbpedia.org/property/otherName>",
    "<http://dbpedia.org/property/alternativeNames>",
    "<http://dbpedia.org/property/otherNames>",
    "<http://dbpedia.org/property/longName>",
    "<http://dbpedia.org/property/conventionalLongName>",
    "<http://dbpedia.org/property/commonName>",
    "<http://dbpedia.org/property/altname>",
    "<http://dbpedia.org/property/glottorefname>",
    "<http://dbpedia.org/property/sname>",
    "<http://dbpedia.org/ontology/longName>",
    "<http://dbpedia.org/ontology/wikiPageWikiLinkText>",

    "<http://www.w3.org/2000/01/rdf-schema#label>"
  ).map(s => s.toLowerCase())

  val blacklist = Seq(
    "<http://www.w3.org/2002/07/owl#differentFrom>",
    "<http://dbpedia.org/property/urlname>",
    "<http://dbpedia.org/property/isbn>",
    "<http://dbpedia.org/property/issn>",
    "<http://dbpedia.org/ontology/isbn>",
    "<http://dbpedia.org/ontology/issn>",
    "<http://dbpedia.org/property/groupstyle>",
    "<http://dbpedia.org/property/align>",
    "<http://dbpedia.org/property/width>",
    "<http://dbpedia.org/property/bgcolor>",
    "<http://dbpedia.org/property/direction>",
    "<http://dbpedia.org/property/headerAlign>",
    "<http://dbpedia.org/property/footerAlign>",
    "<http://dbpedia.org/property/headerBackground>",
    "<http://dbpedia.org/property/voy>",
    "<http://dbpedia.org/property/commons>",
    "<http://dbpedia.org/property/id>",
    "<http://dbpedia.org/property/reason>",
    "<http://dbpedia.org/property/hideroot>"
  ).map(s => s.toLowerCase())

  val subjectPredicate = "<http://purl.org/dc/terms/subject>"

  val wikiLinkTextPredicate = "<http://dbpedia.org/ontology/wikiPageWikiLinkText>"

  val rdfsCommentPredicate = "<http://www.w3.org/2000/01/rdf-schema#comment>"

  val rdfsLabelPredicate = "<http://www.w3.org/2000/01/rdf-schema#label>"

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
      namePredicates.contains(pred.toLowerCase()) && obj.startsWith("\"")
    }.map { case (subj, pred, obj) =>
      (subj, obj)
    }.distinct().cache()

    val namesCompact = names.groupByKey().map { case (subj, names) => (subj, names.head)}

    val categories = triples.filter { case (subj, pred, obj) =>
      pred == subjectPredicate
    }.map { case (subj, pred, obj) =>
      (obj, subj)
    }.join(names).map { case (obj, (subj, objName)) =>
      (subj, objName)
    }.distinct()

    val regularTriples = triples.filter { case (subj, pred, obj) =>
      !namePredicates.contains(pred.toLowerCase()) &&
        pred != subjectPredicate &&
        !blacklist.contains(pred.toLowerCase())
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
    }.join(namesCompact).map { case (obj, ((predName, subj), objName)) =>
      (subj, (predName, objName))
    }.union(filenames).distinct()

    val commentedEntities = triples.filter { case (subj, pred, obj) =>
      pred == rdfsCommentPredicate
    }.map { case (subj, pred, obj) =>
      (subj, true)
    }

    val labelledEntities = triples.filter { case (subj, pred, obj) =>
      pred == rdfsLabelPredicate
    }.map { case (subj, pred, obj) =>
      (subj, true)
    }

    val indexedEntities = commentedEntities.intersection(labelledEntities)

    new CoGroupedRDD(Seq(names, attributes, categories, relatedEntityNames, indexedEntities),
      Partitioner.defaultPartitioner(names, attributes, categories, relatedEntityNames, indexedEntities)).
      mapValues { case Array(namesArray, attributesArray, categoriesArray, relatedEntityNamesArray, indexedEntitiesArray) =>
        (namesArray.asInstanceOf[Seq[String]],
          attributesArray.asInstanceOf[Seq[(Option[String], String)]],
          categoriesArray.asInstanceOf[Seq[String]],
          relatedEntityNamesArray.asInstanceOf[Seq[(Option[String], String)]],
          indexedEntitiesArray.asInstanceOf[Seq[Boolean]])
      }.flatMap { case (entityUri, (namesSeq, attributesSeq, categoriesSeq,
    relatedEntityNamesSeq, indexedEntitiesSeq)) =>
      if (indexedEntitiesSeq.nonEmpty)
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
          (if (relatedEntityNamesSeq.nonEmpty) Array("<relatedentitynames>") ++
            relatedEntityNamesSeq.map {
              case (Some(predName), objName) => extractLiteralText(predName) + " " + extractLiteralText(objName)
              case (None, objName) => extractLiteralText(objName)
            }.distinct ++
            Array("</relatedentitynames>")
          else Seq()) ++
          Array("</TEXT>\n</DOC>")
      else Array[String]()
    }.saveAsTextFile(pathToOutput)
  }
}
