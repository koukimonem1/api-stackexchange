package com.tcb.formation.learning.nearestcentroid

import com.tcb.formation.storage.Question
import com.tcb.formation.storage.DictionaryWord

trait TrainingPhase {
  def filterBagOfWords(): java.util.List[DictionaryWord]
  def getTFIDF(word: DictionaryWord, question: Question): Float
  def getCentroid(label: Int): Map[String, Float]
  def getQuestionVector(question: Question): Map[String, Float]
}