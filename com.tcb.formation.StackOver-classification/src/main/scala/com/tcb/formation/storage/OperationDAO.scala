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
  def getCentroidAcc(label: Int) : Map[String, Float]
  def getDFs(): Map[String, Int]
}