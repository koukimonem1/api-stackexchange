package com.tcb.formation.learning.nearestcentroid

import scala.math.log

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

import com.tcb.formation.storage.DictionaryWord
import com.tcb.formation.storage.OperationDAO
import com.tcb.formation.storage.Question
import com.tcb.formation.storage.hbase.HbaseDAO
import scala.collection.immutable.HashMap

@Component
@Scope("singleton")
class TrainingPhaseImpl extends TrainingPhase {
  
  @Autowired
  val dao: HbaseDAO = null

  def filterBagOfWords(): java.util.List[DictionaryWord] = dao.getBagOfWords

  def getCentroid(label: Int): Map[String, Float] = {
    var centoidSum: Map[String, Float] = new HashMap[String, Float]
    val size = dao.getCentroidAcc(label).size
    dao.getCentroidAcc(label).foreach((couple: (String, Float)) => centoidSum += ((couple._1, couple._2 / size)))
    centoidSum
  }

  def getTFIDF(word: DictionaryWord, question: Question): Float = {
    val tf = dao.getTF(word, question)
    val df = dao.getDF(word)
    tf * log(dao.getNormCorpus / df).toFloat
  }

  def getQuestionVector(question: Question): Map[String, Float] = {
    var vector: Map[String, Float] = new HashMap[String, Float]
    question.body.foreach { word => vector += ((word, getTFIDF(DictionaryWord(word), question))) }
    vector
  }
}