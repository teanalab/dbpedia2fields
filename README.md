Prerequisites:
* sbt
* [Spark 1.2.0](https://archive.apache.org/dist/spark/spark-1.2.0/spark-1.2.0-bin-hadoop2.4.tgz)

Run TriplesToTrec with the following 13 files from DBPedia 2015-10 as input

* anchor_text_en.ttl
* article_categories_en.ttl
* category_labels_en.ttl
* infobox_properties_en.ttl
* infobox_property_definitions_en.ttl
* instance_types_transitive_en.ttl
* labels_en.ttl
* long_abstracts_en.ttl
* mappingbased_literals_en.ttl
* mappingbased_objects_en.ttl
* page_links_en.ttl
* persondata_en.ttl
* short_abstracts_en.ttl

Some Spark parameters tuning is required to run it successfully, for example `--executor-memory 22g --driver-memory 6g --conf spark.yarn.executor.memoryOverhead=1g`.

Example command to run:
```bash
$ sbt assembly
$ $SPARK_HOME/bin/spark-submit --class 'edu.wayne.dbpedia2fields.TriplesToTrec' --master 'local[*]' --executor-memory 22g --driver-memory 6g --conf spark.yarn.executor.memoryOverhead=1g target/scala-2.10/dbpedia2fields-assembly-1.0.jar 'dbpedia-2015-10-subset/*.ttl' triples-to-trec
```
