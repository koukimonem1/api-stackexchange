package com.tcb.formation.learning.nearestcentroid

import com.tcb.formation.storage.Question
import com.tcb.formation.storage.DictionaryWord

trait TrainingPhase {
  def filterBagOfWords(): Seq[String]
  def getTFIDF(word: DictionaryWord, question: Question): Float
  def getCentroid(label: Int): Map[String, Float]
}