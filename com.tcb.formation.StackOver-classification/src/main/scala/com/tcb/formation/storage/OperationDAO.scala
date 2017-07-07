package com.tcb.formation.storage

trait OperationDAO {
  def saveQuestion(question: Question)
  def saveWords(words: Seq[DictionaryWord])
  def saveStopWords(stopwords: Seq[StopWord])
  def getStopWords: java.util.List[StopWord]
  def getBagOfWords: java.util.List[DictionaryWord]
  def createDatabase()
}