package com.tcb.formation.storage

import org.springframework.beans.factory.config.AbstractFactoryBean
import org.springframework.stereotype.Component
import javax.annotation.Resource;
@Component
class OperationDAOFactory {
  def getOperationDao(typeOpeartion: String): OperationDAO = {
    if (typeOpeartion == "hive") {
      @Resource(name = "hiveDAO")
      var operation: OperationDAO = null
      operation
    } else {
      @Resource(name = "hbaseDAO")
      var operation: OperationDAO = null
      operation
    }
  }
}