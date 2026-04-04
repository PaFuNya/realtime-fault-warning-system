package org.example.tasks

import ml.dmlc.xgboost4j.scala.XGBoost
import ml.dmlc.xgboost4j.scala.DMatrix

object TestPredict2 {
  def main(args: Array[String]): Unit = {
    val rulPath = "D:/Desktop/Match/MATCH/OverMatch/Device_Rul_xgboost_v1.bin"
    val rulBooster = XGBoost.loadModel(rulPath)

    val f1 = Array[Float](1.0f, 1.0f, 0.0f, 0.0f, 0.0f, 0.8f, 1.0f, 5.0f, 10.0f, 1000.0f, 500.0f, 0.0f, 10.0f, 0.8f)
    val f2 = Array[Float](2.0f, 1.0f, 0.0f, 0.0f, 0.0f, 1.6f, 2.0f, 5.0f, 10.0f, 1010.0f, 500.0f, 0.0f, 10.0f, 1.6f)
    val f3 = Array[Float](3.0f, 1.0f, 0.0f, 0.0f, 0.0f, 2.4f, 3.0f, 5.0f, 10.0f, 1020.0f, 500.0f, 0.0f, 10.0f, 2.4f)

    val d1 = new DMatrix(f1, 1, f1.length, Float.NaN)
    val d2 = new DMatrix(f2, 1, f2.length, Float.NaN)
    val d3 = new DMatrix(f3, 1, f3.length, Float.NaN)

    println("P1: " + rulBooster.predict(d1)(0)(0))
    println("P2: " + rulBooster.predict(d2)(0)(0))
    println("P3: " + rulBooster.predict(d3)(0)(0))
  }
}
