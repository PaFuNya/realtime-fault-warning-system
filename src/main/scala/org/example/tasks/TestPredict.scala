package org.example.tasks

import ml.dmlc.xgboost4j.scala.XGBoost
import ml.dmlc.xgboost4j.scala.DMatrix

object TestPredict {
  def main(args: Array[String]): Unit = {
    val rulPath = "D:/Desktop/Match/MATCH/OverMatch/Device_Rul_xgboost_v1.bin"
    val rulBooster = XGBoost.loadModel(rulPath)

    val arr1 = Array[Float](1.0f, 1.0f, 0.0f, 0.0f, 0.0f, 0.8f, 1.0f, 5.0f,
      10.0f, 1000.0f, 500.0f, 0.0f, 10.0f, 0.8f)
    val d1 = new DMatrix(arr1, 1, arr1.length)
    val pred1 = rulBooster.predict(d1)
    println("Pred 1: " + pred1(0)(0))

    val arr2 = Array[Float](100.0f, 1.0f, 0.0f, 0.0f, 0.0f, 80.0f, 100.0f,
      5000.0f, 100.0f, 100000.0f, 50000.0f, 0.0f, 100.0f, 80.0f)
    val d2 = new DMatrix(arr2, 1, arr2.length)
    val pred2 = rulBooster.predict(d2)
    println("Pred 2: " + pred2(0)(0))
  }
}
