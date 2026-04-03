package edu.cmp26.pipeline

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame

class MyPipeline {
  var pipeline:Pipeline = null
  var pipelineModel:PipelineModel = null

  //最大分类树（用于识别连续值特征和分类特征）
  private var maxCategories=5
  // 最大分支数
  private var maxBins=5
  // 最大树深度
  private var maxDepth=5
  //最小分支包含数据条数
  private var minInstancesPerNode=1
  //最小分支信息增益
  private var minInfoGain=0.0
  def setMaxCategories(maxCategories:Int): MyPipeline ={
    this.maxCategories=maxCategories
    this
  }
  def setMaxBins(maxBins:Int): MyPipeline ={
    this.maxBins=maxBins
    this
  }
  def setMaxDepth(maxDepth:Int): MyPipeline ={
    this.maxDepth=maxDepth
    this
  }
  def setMinInstancesPerNode(minInstancesPerNode:Int): MyPipeline ={
    this.minInstancesPerNode=minInstancesPerNode
    this
  }
  def setMinInfoGain(minInfoGain:Double): MyPipeline ={
    this.minInfoGain=minInfoGain
    this
  }

  var labelColName:String=null
  var featureColNames:Array[String]=null

  def init(): Unit = {
    pipeline = new Pipeline().setStages(Array(
      createLabelIndexer,
      createFeatureCols,
      createFeatureIndexer,
      createClassifier
    ))
  }
  def setLabelColName(name:String): Unit = {
    labelColName=name
  }
  def setFeatureColNames(names:Array[String]): Unit = {
    featureColNames=names
  }
 //1 创建标签索引，把标签值转换为矢量值
  //InputCol:参考答案列名
  //OutputCol:转换为矢量值的列名
 def createLabelIndexer(): StringIndexer = {
   val labelIndexer = new StringIndexer()
     .setInputCol("label")
     .setOutputCol("indexedLabel")
     labelIndexer
 }

 //2 创建特征集合列
 //InputCol:特征列名
 //OutputCol:特征集合列名
  def createFeatureCols(): VectorAssembler = {
    val assembler =new VectorAssembler()
    assembler.setInputCols(featureColNames)
      .setOutputCol("feature_assemble")
    assembler
  }
  //3 创建特征索引列
  //把特征集合中的原值，变为 矢量值 按照 出现概率大小次序  概率越大矢量越小
  //要识别哪些是连续值特征（线性特征），哪些是离散值特征（分类特征）
  def createFeatureIndexer(): VectorIndexer = {
    val vectorIndexer =new VectorIndexer()
    vectorIndexer.setInputCol("feature_assemble")
      .setOutputCol("feature_index")
      .setMaxCategories(maxCategories)   //超过连续，低于离散
  }
  //4 创建分类器
  def createClassifier(): DecisionTreeClassifier = {
    val classifier = new DecisionTreeClassifier()
    classifier.setLabelCol("indexedLabel")
      .setFeaturesCol("feature_index")
      .setPredictionCol("prediction_col")
      .setImpurity("gini")//信息熵  基尼
      .setMaxBins(maxBins)
      .setMaxDepth(maxDepth)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setMinInfoGain(minInfoGain)
    classifier
  }
  //训练
  def train(dataFrame:DataFrame): Unit = {
    pipelineModel = pipeline.fit(dataFrame)
  }

  //预测
  def predict(dataFrame:DataFrame): DataFrame = {
    val predictDataFrame:DataFrame = pipelineModel.transform(dataFrame)
    predictDataFrame
  }
}
