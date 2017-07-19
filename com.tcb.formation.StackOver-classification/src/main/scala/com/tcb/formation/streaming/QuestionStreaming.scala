package com.tcb.formation.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import com.tcb.formation.ApplicationConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.tcb.formation.learning.nearestcentroid.RocchioClassification
import com.tcb.formation.storage.Question
import com.tcb.formation.storage.hbase.HbaseDAO

class QuestionStreaming {
  def main(args: Array[String]): Unit = {
    val springContext = new AnnotationConfigApplicationContext(classOf[ApplicationConfig])
    val sc = springContext.getBean("getSparkSession", classOf[SparkSession]).sparkContext
    val classifier = springContext.getBean(classOf[RocchioClassification])
    val dao = springContext.getBean(classOf[HbaseDAO])
    val ssc = new StreamingContext(sc, Seconds(30))
    val customReceiverStream = ssc.receiverStream(new CustomQuestionReceiver)
    customReceiverStream.foreachRDD {
      rddQuestion =>
        rddQuestion.foreach {
          question =>
            val label = classifier.deducateClass(question)
            var newQuestion = Question(question.id, question.body, question.tags, label)
            dao.saveQuestion(newQuestion)
        }
    }
  }
}