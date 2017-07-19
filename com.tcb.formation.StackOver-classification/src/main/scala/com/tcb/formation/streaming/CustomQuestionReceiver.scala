package com.tcb.formation.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import com.tcb.formation.services.QuestionService
import com.tcb.formation.storage.Question

@Component
class CustomQuestionReceiver extends Receiver[Question](StorageLevel.MEMORY_AND_DISK_2) {
  @Autowired
  val service: QuestionService = null
  def onStart() {
    new Thread("Get question") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
  }
  def receive() = {
    val ids = service.getStreamingIds
    val itr = ids.iterator()
    while (itr.hasNext()) {
      val id = itr.next
      val question = service.getQuestion(id.toLong, 5, "hbase")
      store(question)
      Thread.sleep(10000)
    }
  }
}