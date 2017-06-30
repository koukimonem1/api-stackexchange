package com.tcb.formation.storage

case class DictionaryWord(word: String)

case class Question(id: Long, body: Seq[String], tags: Seq[String], label: Int)

case class StopWord(stopword: String)
