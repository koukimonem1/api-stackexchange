package com.tcb.formation.storage

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Scope
import org.springframework.stereotype.Component

import com.tcb.formation.storage.hbase.HbaseDAO
import com.tcb.formation.storage.hive.HiveDAO

@Component
class OperationDAOFactory {
  @Autowired
  val operationHive: HiveDAO = null
  @Autowired
  val operationHbase: HbaseDAO = null
  def getOperationDao(typeOpeartion: String): OperationDAO = if (typeOpeartion == "hive") operationHive else operationHbase
}