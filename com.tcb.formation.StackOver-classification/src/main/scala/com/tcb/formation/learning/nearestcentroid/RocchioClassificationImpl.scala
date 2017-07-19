package com.tcb.formation.learning.nearestcentroid

import com.tcb.formation.storage.Question
import org.springframework.stereotype.Component
import org.springframework.context.annotation.Scope
import org.springframework.beans.factory.annotation.Autowired
import scala.math._

@Component
@Scope("singleton")
class RocchioClassificationImpl extends RocchioClassification {
  @Autowired
  val training: TrainingPhase = null

  def distance(question: Question, label: Int): Float = {
    var distance: Float = 0
    val vectorQuestion = training.getQuestionVector(question)
    val vectorCentroid = training.getCentroid(label)
    vectorCentroid.foreach { (couple: (String, Float)) => { distance = distance + pow(couple._2 - vectorQuestion(couple._1), 2).toFloat } }
    sqrt(distance).toInt
  }

  def deducateClass(question: Question): Int = {
    val class_0 = distance(question, 0)
    val class_1 = distance(question, 1)
    if (class_0 < class_1) 0 else 1
  }

}