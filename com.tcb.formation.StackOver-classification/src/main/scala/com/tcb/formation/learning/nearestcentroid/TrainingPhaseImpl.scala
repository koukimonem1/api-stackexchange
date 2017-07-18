package com.tcb.formation.learning.nearestcentroid

import scala.math.log

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

import com.tcb.formation.storage.DictionaryWord
import com.tcb.formation.storage.OperationDAO
import com.tcb.formation.storage.Question
import com.tcb.formation.storage.hbase.HbaseDAO

@Component
@Scope("singleton")
class TrainingPhaseImpl extends TrainingPhase {
  @Autowired
  val dao: HbaseDAO = null
  
  def filterBagOfWords(): Seq[String] = ???
  
  def getCentroid(label: Int): Map[String, Float] = ???
  
  def getTFIDF(word: DictionaryWord, question: Question): Float = {
    val tf = dao.getTF(word, question)
    val df = dao.getDF(word)
    tf.toFloat * dao.getNormCorpus().toFloat / log(df.toDouble).toFloat
  }
}