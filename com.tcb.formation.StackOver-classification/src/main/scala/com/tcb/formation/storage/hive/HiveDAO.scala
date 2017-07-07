package com.tcb.formation.storage.hive

import scala.reflect.runtime.universe

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Service

import com.tcb.formation.storage.DictionaryWord
import com.tcb.formation.storage.Question
import com.tcb.formation.storage.StopWord
import com.tcb.formation.storage.OperationDAO
import org.springframework.stereotype.Component

@Component
class HiveDAO extends OperationDAO {

  @Autowired
  val spark: SparkSession = null

  import spark.implicits._
  import spark.sql

  /**
   * tables name
   *
   */
  @Value("${warehouse.question.table}")
  val questionTable: String = null
  @Value("${warehouse.bagofwords.table}")
  val bowTable: String = null
  @Value("${warehouse.stopwords.table}")
  val stopwordTable: String = null

  def saveQuestion(question: Question) = spark.createDataset(Seq(question)).write.mode(SaveMode.Append).insertInto(questionTable)

  def saveWords(words: Seq[DictionaryWord]) = words.toDS().write.mode(SaveMode.Append).insertInto(bowTable);

  def saveStopWords(stopwords: Seq[StopWord]) = stopwords.toDS().write.mode(SaveMode.Append).insertInto(stopwordTable);

  def getStopWords: java.util.List[StopWord] = sql("select * from so_classification.stopwords").as[StopWord].collectAsList

  def getBagOfWords: java.util.List[DictionaryWord] = sql("select * from so_classification.Dictionnaire").as[DictionaryWord].collectAsList

  def stopWordExist(word: String): Boolean = sql(s"select * from $stopwordTable").filter(sw => sw != word).count() > 0

  def createDatabase = HiveCreateDatabase.execute()

}