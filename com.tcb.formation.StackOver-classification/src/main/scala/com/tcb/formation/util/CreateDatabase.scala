package com.tcb.formation.util

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired

import com.tcb.formation.storage.hive.HiveDAO
import com.tcb.formation.storage.StopWord
import com.tcb.formation.storage.hive.HiveDAOImpl
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import com.tcb.formation.ApplicationConfig
import scala.collection.JavaConversions
import org.springframework.beans.factory.annotation.Value
import javax.annotation.PostConstruct
import org.springframework.stereotype.Component
import org.springframework.context.annotation.Scope

class CreateDatabase()

@Component
@Scope("singleton")
object CreateDatabase {
  @Autowired
  var dao: HiveDAO = null
  @Value("${warehouse.database}")
  var warehouseDir: String = null
  @Autowired
  val spark: SparkSession = null

  def execute() {

      var stopwords: java.util.List[StopWord] = new java.util.ArrayList[StopWord]

      import spark.sql
      import spark.implicits._
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