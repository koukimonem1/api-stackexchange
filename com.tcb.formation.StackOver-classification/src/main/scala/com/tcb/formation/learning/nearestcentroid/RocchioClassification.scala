package com.tcb.formation.learning.nearestcentroid

import com.tcb.formation.storage.Question

trait RocchioClassification {
  def distance(question: Question, label: Int) : Float
  def deducateClass(question: Question) : Int
}