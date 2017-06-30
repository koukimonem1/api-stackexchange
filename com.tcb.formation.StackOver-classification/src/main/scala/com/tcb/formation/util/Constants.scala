package com.tcb.formation.util

import java.util.ResourceBundle

object Constants {

  def getConstants(key: String): String = {
    val bundle = ResourceBundle.getBundle("config")
    val value = bundle.getString(key)
    value
  }

}