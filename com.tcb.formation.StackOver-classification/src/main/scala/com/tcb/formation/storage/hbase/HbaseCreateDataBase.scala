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


class HbaseCreateDataBase()

@Component
@Scope("singleton")
object HbaseCreateDataBase{
  @Autowired
  val session: SparkSession = null
  def init: Unit = {
    val connection = ConnectionFactory.createConnection(session.sparkContext.hadoopConfiguration)

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
    // close hbase connection
    connection.close()

    val remover = new StopWordsRemover()
    val stopWords = remover.getStopWords.filter(word => !word.equals("how"))
    session.sparkContext.parallelize(stopWords).saveAsTextFile("hdfs://127.0.1.1:8020/user/kouki/stopwords")
  }
}
