package com.tcb.formation.storage.hbase

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

import com.tcb.formation.storage.DictionaryWord
import com.tcb.formation.storage.OperationDAO
import com.tcb.formation.storage.StopWord
import org.springframework.beans.factory.annotation.Value
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Get
import parquet.format.KeyValue
import org.apache.hadoop.hbase.KeyValue

@Component
@Scope("singleton")
class HbaseDAO extends OperationDAO with Serializable {

  @Autowired
  val session: SparkSession = null
  @Autowired
  val job: Job = null
  @Value("${stopwords.path.hdfs}")
  val pathSW: String = null
  import session.implicits._
  def createDatabase(): Unit = HbaseCreateDataBase.init

  def getBagOfWords: java.util.List[com.tcb.formation.storage.DictionaryWord] = {
    var listWords: java.util.List[com.tcb.formation.storage.DictionaryWord] = new java.util.ArrayList[com.tcb.formation.storage.DictionaryWord]
    session.sparkContext.hadoopConfiguration.set(TableInputFormat.INPUT_TABLE, "dictionary")
    val hBaseRDDResult = session.sparkContext
      .newAPIHadoopRDD(session.sparkContext.hadoopConfiguration, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val bwRDD = hBaseRDDResult.values.map { result => result.getValue(Bytes.toBytes("df"), Bytes.toBytes("df")) }
    bwRDD.map(word => listWords.add(DictionaryWord(word.toString)))
    listWords
  }

  def incrementDf(word: DictionaryWord): Array[Byte] = {
    val hTable = new HTable(job.getConfiguration, "dictionary")
    val get = new Get(word.word.getBytes)
    val result = hTable.get(get);
    result.getValue(Bytes.toBytes("df"), Bytes.toBytes("df"))
  }

  def getStopWords: java.util.List[com.tcb.formation.storage.StopWord] = {
    var listSW: java.util.List[com.tcb.formation.storage.StopWord] = new java.util.ArrayList[com.tcb.formation.storage.StopWord]
    session.sparkContext.wholeTextFiles(pathSW).map { case (path, sw) => listSW.add(StopWord(sw)) }
    listSW
  }

  def saveQuestion(question: com.tcb.formation.storage.Question): Unit = {
    job.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "question")
    var questionRDD = session.sparkContext.parallelize(Seq(question)).map { q =>
      var put = new Put(Bytes.toBytes(q.label + "-" + q.id))
      put.addColumn("identity".getBytes(), "id".getBytes(), Bytes.toBytes(q.id))
      put.addColumn("identity".getBytes(), "label".getBytes(), Bytes.toBytes(q.label))
      q.body.foreach { word => put.addColumn("body".getBytes(), word.getBytes(), Bytes.toBytes(q.body.count { w => w.equals(word) }.toString())) }
      q.tags.foreach { tag => put.addColumn("tags".getBytes(), tag.getBytes(), Bytes.toBytes("1")) }
      put
    }
    var questionHbasePut = questionRDD.map { questionPut => (new ImmutableBytesWritable, questionPut) }
    questionHbasePut.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def saveStopWords(stopwords: Seq[com.tcb.formation.storage.StopWord]): Unit = session.createDataset(stopwords).write.mode(SaveMode.Append).text(pathSW)

  def saveWords(words: Seq[com.tcb.formation.storage.DictionaryWord]): Unit = {
    job.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "dictionary")
    var wordsRDD = session.sparkContext.parallelize(words)
      .filter { word => word.word != null && !word.word.equals("") && !word.word.equals(" ") && !word.word.equals("\n") }
      .map { word =>
        var put = new Put(Bytes.toBytes(word.word))
        put.addColumn("df".getBytes(), "df".getBytes(), "1".getBytes)
      }
    var wordsHbasePut = wordsRDD.map { wordPut => (new ImmutableBytesWritable, wordPut) }
    wordsHbasePut.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}