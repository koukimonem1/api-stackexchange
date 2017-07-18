package com.tcb.formation.storage

trait OperationDAO {
  def saveQuestion(question: Question)
  def saveWords(words: Seq[DictionaryWord])
  def saveStopWords(stopwords: Seq[StopWord])
  def getStopWords: java.util.List[StopWord]
  def getBagOfWords: java.util.List[DictionaryWord]
  def createDatabase()
  def getDF(word: DictionaryWord): Int
  def getTF(word: DictionaryWord, question: Question): Int
  def getNormCorpus(): Int
  def getCentroid(label: Int) : Map[String, Int]
  def getDFs(): Map[String, Float]
}