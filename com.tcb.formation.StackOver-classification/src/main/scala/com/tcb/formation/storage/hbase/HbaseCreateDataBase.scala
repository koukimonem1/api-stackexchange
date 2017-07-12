package com.tcb.formation.storage.hbase

import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component
import org.apache.hadoop.mapreduce.Job
import com.tcb.formation.storage.hive.HiveDAO
import com.tcb.formation.storage.StopWord
import scala.collection.JavaConversions

class HbaseCreateDataBase()

@Component
@Scope("singleton")
object HbaseCreateDataBase {
  @Autowired
  val session: SparkSession = null
  @Autowired
  val job: Job = null
  @Autowired
  val hbaseDao: HbaseDAO = null

  def init: Unit = {
    val connection = ConnectionFactory.createConnection(job.getConfiguration)
    var stopwordsList: java.util.List[StopWord] = new java.util.ArrayList[StopWord]
    /**
     * Create question table
     */

    val questionTable = new HTableDescriptor(TableName.valueOf("question"))
    questionTable.addFamily(new HColumnDescriptor("identity"))
    questionTable.addFamily(new HColumnDescriptor("body"))
    questionTable.addFamily(new HColumnDescriptor("tags"))
    connection.getAdmin.createTable(questionTable)

    /**
     * Create dictionary table
     */

    val dictionary = new HTableDescriptor(TableName.valueOf("dictionary"))
    dictionary.addFamily(new HColumnDescriptor("df"))
    connection.getAdmin.createTable(dictionary)

    /**
     * Create stopwords table
     */

    val stopwords = new HTableDescriptor(TableName.valueOf("stopwords"))
    stopwords.addFamily(new HColumnDescriptor("exist"))
    connection.getAdmin.createTable(stopwords)

    val remover = new StopWordsRemover()
    val stopWords = remover.getStopWords.filter(word => !word.equals("how"))

    stopWords.foreach { word =>
      val stopWord = StopWord((word))
      stopwordsList add stopWord
    }
    hbaseDao.saveStopWords(JavaConversions.asScalaBuffer(stopwordsList).seq)
    // close hbase connection
    connection.close()
  }
}
