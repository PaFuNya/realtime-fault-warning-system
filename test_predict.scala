import biz.k11i.xgboost.Predictor
import biz.k11i.xgboost.util.FVec
import java.io.FileInputStream

val rulPath = "D:/Desktop/Match/MATCH/OverMatch/Device_Rul_xgboost_v1.bin"
val rulPredictor = new Predictor(new FileInputStream(rulPath))

val rulFeatureArray = Array[Double](0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1000.0, 0.0, 0.0, 0.0, 0.0)
val rulVec = FVec.Transformer.fromArray(rulFeatureArray, true)
val rulPreds = rulPredictor.predict(rulVec)
println("RUL length: " + rulPreds.length)
println("RUL value: " + rulPreds.mkString(","))

val faultPath = "D:/Desktop/Match/MATCH/OverMatch/fault_probability_xgboost_v2.bin"
val faultPredictor = new Predictor(new FileInputStream(faultPath))
val faultFeatureArray = Array[Double](85.0, 1.0, 0.5, 10.0)
val faultVec = FVec.Transformer.fromArray(faultFeatureArray, true)
val faultPreds = faultPredictor.predict(faultVec)
println("Fault length: " + faultPreds.length)
println("Fault value: " + faultPreds.mkString(","))

