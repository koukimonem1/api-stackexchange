package com.tcb.formation.storage.hbase

import com.tcb.formation.storage.Question
import com.tcb.formation.storage.OperationDAO
import org.springframework.stereotype.Component
import org.springframework.context.annotation.Scope
import org.apache.spark.SparkContext
import org.springframework.beans.factory.annotation.Autowired
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.function.PairFunction
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapred.TableOutputFormat

class HbaseDao extends OperationDAO {
  @Autowired
  val sc: SparkContext = null

  def createDatabase(): Unit = HbaseCreateDataBase.init
  def getBagOfWords: java.util.List[com.tcb.formation.storage.DictionaryWord] = ???
  def getStopWords: java.util.List[com.tcb.formation.storage.StopWord] = ???
  def saveQuestion(question: com.tcb.formation.storage.Question): Unit = ???
  def saveStopWords(stopwords: Seq[com.tcb.formation.storage.StopWord]): Unit = ???
  def saveWords(words: Seq[com.tcb.formation.storage.DictionaryWord]): Unit = {
    val job = Job.getInstance(sc.hadoopConfiguration)
    job.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "dictionary")
    var wordsRDD = sc.parallelize(words).map { word =>
      var put = new Put(Bytes.toBytes(word.word));
      put.addColumn("df".getBytes(), "df".getBytes(), Bytes.toBytes(1));
    }
    var wordsHbasePut = wordsRDD.map { wordPut => (new ImmutableBytesWritable, wordPut) }
    wordsHbasePut.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}