package com.tcb.formation.storage.hive

import com.tcb.formation.storage.DictionaryWord
import com.tcb.formation.storage.Question
import com.tcb.formation.storage.StopWord

trait HiveDAO {
  def saveQuestion(question: Question)
  def saveWords(words: Seq[DictionaryWord])
  def saveStopWords(stopwords: Seq[StopWord])
  def stopWordExist(word: String): Boolean
  def getStopWords: java.util.List[StopWord]
  def getBagOfWords: java.util.List[DictionaryWord]
  def createDatabase()
}