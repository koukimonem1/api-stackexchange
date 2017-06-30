package com.tcb.formation.storage

import scala.reflect.runtime.universe

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Service
import com.tcb.formation.util.CreateDatabase

@Service
@Scope("singleton")
class HiveDAOImpl extends HiveDAO {

  @Autowired
  val spark: SparkSession = null
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

  def saveQuestion(question: Question) {

    import spark.implicits._

    val questionDS = spark.createDataset(Seq(question))
    questionDS.show()
    questionDS.write
      .mode(SaveMode.Append)
      .insertInto(questionTable);
  }

  def saveWords(words: Seq[DictionaryWord]) {

    import spark.implicits._

    val wordDS = words.toDS()
    wordDS.write
      .mode(SaveMode.Append)
      .insertInto(bowTable);
  }

  def saveStopWords(stopwords: Seq[StopWord]) {

    import spark.implicits._

    val wordDS = stopwords.toDS()
    wordDS.write
      .mode(SaveMode.Append)
      .insertInto(stopwordTable);
  }

  def getStopWords: java.util.List[StopWord] = {

    import spark.implicits._
    import spark.sql

    val stopWords = sql("select * from so_classification.stopwords").as[StopWord]
    stopWords.collectAsList()
  }

  def getBagOfWords(): java.util.List[DictionaryWord] = {

    import spark.implicits._
    import spark.sql

    val stopWords = sql("select * from so_classification.Dictionnaire").as[DictionaryWord]
    stopWords.collectAsList()
  }

  def stopWordExist(word: String): Boolean = {

    import spark.sql

    val exist = sql(s"select * from $stopwordTable").filter(sw => sw != word).count() > 0
    exist
  }
  
  def createDatabase() = CreateDatabase.execute()
  
}