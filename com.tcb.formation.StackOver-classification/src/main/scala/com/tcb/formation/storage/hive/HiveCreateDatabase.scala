package com.tcb.formation.storage.hive

import scala.collection.JavaConversions

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import com.tcb.formation.storage.StopWord

class HiveCreateDatabase()

@Component
@Scope("singleton")
object HiveCreateDatabase {
  @Autowired
  var dao: HiveDAO = null
  @Value("${warehouse.database}")
  var warehouseDir: String = null
  @Autowired
  val spark: SparkSession = null

  def execute() {

      var stopwords: java.util.List[StopWord] = new java.util.ArrayList[StopWord]

      import spark.sql
      sql("CREATE DATABASE IF NOT EXISTS so_classification").write
      sql("CREATE TABLE IF NOT EXISTS so_classification.question( id INT,body ARRAY<STRING>,  tags ARRAY<STRING>,label INT) ROW FORMAT DELIMITED  FIELDS TERMINATED BY ',' COLLECTION ITEMS TERMINATED BY '-'  STORED AS TEXTFILE").write
      sql("CREATE TABLE IF NOT EXISTS so_classification.Dictionnaire(word STRING)").write
      sql("CREATE TABLE IF NOT EXISTS so_classification.stopwords(stopword STRING)").write

      val remover = new StopWordsRemover()
      val stopWords = remover.getStopWords.filter(word => !word.equals("how"))

      stopWords.foreach { word =>
        val stopWord = StopWord((word))
        stopwords add stopWord
      }
      dao.saveStopWords(JavaConversions.asScalaBuffer(stopwords).seq)
  }
}