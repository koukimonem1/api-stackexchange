package com.tcb.formation.storage

import org.apache.spark.sql.SparkSession
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

trait HiveDAO {
  def saveQuestion(question: Question)
  def saveWords(words: Seq[DictionaryWord])
  def saveStopWords(stopwords: Seq[StopWord])
  def stopWordExist(word: String): Boolean
  def getStopWords: java.util.List[StopWord]
  def getBagOfWords: java.util.List[DictionaryWord]
  def createDatabase()
}