package com.tcb.formation.util

import org.apache.spark.util.AccumulatorV2
import java.util.Collections

class ListAccumulator[T] extends AccumulatorV2[java.util.List[T], java.util.List[T]] {

  var list: java.util.List[T] = new java.util.ArrayList[T]

  def reset: Unit = list.clear()

  def add(list: java.util.List[T]) = this.list.addAll(list)

  def copy(): AccumulatorV2[java.util.List[T], java.util.List[T]] = {
    var list: java.util.List[T] = new java.util.ArrayList[T]
    Collections.copy(new java.util.ArrayList[T], list)
    var acc: ListAccumulator[T] = new ListAccumulator[T]
    acc.list = list
    acc
  }

  def isZero: Boolean = list.isEmpty()

  def merge(other: AccumulatorV2[java.util.List[T], java.util.List[T]]) = {}

  def value = list

}