package com.tcb.formation.storage.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HBaseAdmin
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
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

import com.tcb.formation.storage.DictionaryWord
import com.tcb.formation.storage.OperationDAO
import com.tcb.formation.storage.Question
import com.tcb.formation.storage.StopWord
import scala.collection.immutable.HashMap

@Component
@Scope("singleton")
class HbaseDAO extends OperationDAO with java.io.Serializable {

  @Autowired
  val spark: SparkSession = null
  @Autowired
  val job: Job = null
  @Value("${stopwords.path.hdfs}")
  val pathSW: String = null

  def createDatabase(): Unit = HbaseCreateDataBase.init

  def getBagOfWords: java.util.List[com.tcb.formation.storage.DictionaryWord] = {
    val conf = HBaseConfiguration.create()
    val tableName = "dictionary"
    conf.set("hbase.zookeeper.quorum", "127.0.1.1")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      admin.createTable(tableDesc)
    }
    var listWords: java.util.List[com.tcb.formation.storage.DictionaryWord] = new java.util.ArrayList[com.tcb.formation.storage.DictionaryWord]
    val hBaseRDDResult = spark.sparkContext
      .newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    hBaseRDDResult.values.map { result => result.getValue(Bytes.toBytes("df"), Bytes.toBytes("df")) }.collect().foreach { word => listWords.add(DictionaryWord(new String(word))) }
    listWords
  }

  def getDF(word: DictionaryWord): Int = {
    val connection = ConnectionFactory.createConnection(job.getConfiguration)
    val table = connection.getTable(TableName.valueOf("dictionary"))
    val g = new Get(Bytes.toBytes(word.word))
    val r = table.get(g)
    var df: Array[Byte] = null
    if (r != null)
      df = r.getValue(Bytes.toBytes("df"), Bytes.toBytes("df"))
    new String(df).toInt
  }

  def getTF(word: DictionaryWord, question: Question): Int = ???

  def getDFs(): Map[String, Int] = {
    var vectorDF: Map[String, Int] = new HashMap[String, Int]
    val conf = HBaseConfiguration.create()
    val tableName = "dictionary"
    conf.set("hbase.zookeeper.quorum", "127.0.1.1")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      admin.createTable(tableDesc)
    }
    val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    hBaseRDD.values
      .map(res => (new String(res.getRow), new String(res.getValue(Bytes.toBytes("df"), Bytes.toBytes("df"))).toInt))
      .filter(wordDF => wordDF._2 > 1)
      .foreach { vectorDF += (_) }
    vectorDF
  }

  def getCentroid(label: Int): Map[String, Int] = ???

  def getStopWords: java.util.List[com.tcb.formation.storage.StopWord] = {
    val conf = HBaseConfiguration.create()
    val tableName = "stopwords"
    conf.set("hbase.zookeeper.quorum", "127.0.1.1")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      admin.createTable(tableDesc)
    }
    val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    var listSW: java.util.List[com.tcb.formation.storage.StopWord] = new java.util.ArrayList[com.tcb.formation.storage.StopWord]
    hBaseRDD.values.map(res => res.getValue(Bytes.toBytes("exist"), Bytes.toBytes("exist"))).collect().foreach { sw => listSW.add(StopWord(new String(sw))) }
    listSW
  }

  def getNormCorpus(): Int = {
    val conf = HBaseConfiguration.create()
    val tableName = "question"
    conf.set("hbase.zookeeper.quorum", "127.0.1.1")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      admin.createTable(tableDesc)
    }
    val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    hBaseRDD.values.map(res => res.getValue(Bytes.toBytes("identity"), Bytes.toBytes("id"))).count().toInt
  }

  def saveQuestion(question: com.tcb.formation.storage.Question): Unit = {
    job.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "question")
    var questionRDD = spark.sparkContext.parallelize(Seq(question)).map { q =>
      var put = new Put(Bytes.toBytes(q.label + "-" + q.id))
      put.addColumn("identity".getBytes(), "id".getBytes(), Bytes.toBytes(q.id.toString))
      put.addColumn("identity".getBytes(), "label".getBytes(), Bytes.toBytes(q.label.toString))
      q.body.foreach { word => put.addColumn("body".getBytes(), word.getBytes(), Bytes.toBytes(q.body.count { w => w.equals(word) }.toString())) }
      q.tags.foreach { tag => put.addColumn("tags".getBytes(), tag.getBytes(), Bytes.toBytes("1")) }
      put
    }
    var questionHbasePut = questionRDD.map { questionPut => (new ImmutableBytesWritable, questionPut) }
    questionHbasePut.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def saveStopWords(stopwords: Seq[com.tcb.formation.storage.StopWord]): Unit = {
    job.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "stopwords")
    var swRDD = spark.sparkContext.parallelize(stopwords).map { sw =>
      var put = new Put(Bytes.toBytes(sw.stopword))
      put.addColumn("exist".getBytes(), "exist".getBytes(), Bytes.toBytes(sw.stopword))
      put
    }
    var stopwordHbasePut = swRDD.map { questionPut => (new ImmutableBytesWritable, questionPut) }
    stopwordHbasePut.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def saveWords(words: Seq[com.tcb.formation.storage.DictionaryWord]): Unit = {
    val connection = ConnectionFactory.createConnection(job.getConfiguration)
    val table = connection.getTable(TableName.valueOf("dictionary"))
    words.distinct
      .filter { word => !word.word.equals("") && !word.word.equals(" ") && !word.word.equals("\n") && word.word != null }
      .foreach { word =>
        val g = new Get(Bytes.toBytes(word.word))
        val r = table.get(g)
        var oldDF: Array[Byte] = null
        if (r != null)
          oldDF = r.getValue(Bytes.toBytes("df"), Bytes.toBytes("df"))
        val newDF = if (oldDF != null) (new String(oldDF).toLong + 1).toString() else "1"
        val put = new Put(word.word.getBytes)
        put.addColumn("df".getBytes, "df".getBytes, newDF.getBytes)
        table.put(put)
      }
    connection.close()
  }
}