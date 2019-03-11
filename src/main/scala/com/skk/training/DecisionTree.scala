package com.skk.training
import org.apache.spark.ml.{ PipelineModel, Pipeline }
import org.apache.spark.ml.classification.{
  DecisionTreeClassifier,
  RandomForestClassifier,
  RandomForestClassificationModel
}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ VectorAssembler, VectorIndexer }
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{ ParamGridBuilder, TrainValidationSplit }
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import scala.util.Random

object DecisionTree extends App {
  val spark = SparkSession.builder().appName("DecisionTreesAndRandomForests").master("local").getOrCreate()
  spark.conf.set("driver-memory", "4G")
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  //Covtype dataset publicly available dataset provides information on
  //types of forest-covering parcels of land in Colorado, USA
  val fileLoc = "covtype.data"
  val dataWithoutHeader = spark.read.
    option("inferSchema", true).
    option("header", false).
    csv(fileLoc)

  // columns 10 to 14 are for wilderness_area and next 40 columns for soil type
  val colNames = Seq(
    "Elevation", "Aspect", "Slope",
    "Horizontal_Distance_To_Hydrology", "Vertical_Distance_To_Hydrology",
    "Horizontal_Distance_To_Roadways",
    "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
    "Horizontal_Distance_To_Fire_Points") ++ (
      (0 until 4).map(i => s"Wilderness_Area_$i")) ++ (
        (0 until 40).map(i => s"Soil_Type_$i")) ++ Seq("Cover_Type")

  // lets create the data frame wiht column names
  // and cast the label that we have to forecast to double
  val data = dataWithoutHeader.toDF(colNames: _*).
    withColumn("Cover_Type", $"Cover_Type".cast("double"))

  println("\nTake a look at the data")
  data.show()
  data.head

  // Split into 90% train (+ CV), 10% test
  val Array(trainData, testData) = data.randomSplit(Array(0.9, 0.1))
  trainData.cache()
  testData.cache()

  println("-----SIMPLE DECISION TREE------")
  simpleDecisionTree(trainData, testData)

  println("------RANDOM CLASSIFIER PROBABILITIES--------")
  val trainProbabilities = classProbabilities(trainData)
  val testProbabilities = classProbabilities(testData)
  val randomClassificationProbability = trainProbabilities.zip(testProbabilities).map(
    x => x._1 * x._2).sum
  println("\nRandom classification prbability - " + randomClassificationProbability)
  println("\nNow using pipelines and grids to find the best fit")
  evaluate(trainData, testData)
  println("\nNow looking at unonehotencoded run to have a more intuitive model come out")
  evaluateCategorical(trainData, testData)

  println("-------RANDOM FOREST-----------")
  evaluateForest(trainData, testData)

  trainData.unpersist()
  testData.unpersist()

  def simpleDecisionTree(trainData: DataFrame, testData: DataFrame): Unit = {

    val inputCols = trainData.columns.filter(_ != "Cover_Type")
    // spark ml expects features to be a vector
    // we use a transformer - the vector assembler - set its components
    val assembler = new VectorAssembler().
      setInputCols(inputCols).
      setOutputCol("featureVector")

    //and use the vector assembler transformer to transform the training data
    val assembledTrainData = assembler.transform(trainData)
    println("\nThe feature vector produced by the assembler")
    assembledTrainData.select("featureVector").show(truncate = false)

    // the classifier is an estimator whose parameters we set
    val classifier = new DecisionTreeClassifier().
      setSeed(Random.nextLong()).
      setLabelCol("Cover_Type").
      setFeaturesCol("featureVector").
      setPredictionCol("prediction")

    // we fit the estimator to the trained data prepared in a format it accepts
    // and get a decision tree model which itself is a transformer
    val model = classifier.fit(assembledTrainData)
    println("\nThe model printed out")
    println(model.toDebugString)

    println("\nThe feature importances that are provided by the model")
    // model provides column numbers - lets zip with column names to put comprehensible stuff
    model.featureImportances.toArray.zip(inputCols).
      sorted.reverse.foreach(println)

    // we use the model transformer to get predictions from training data
    val predictions = model.transform(assembledTrainData)

    println("\nTake a look at the predictions")
    predictions.select("Cover_Type", "prediction", "probability").
      show(truncate = false)

    // now lets evaluate - initialize an evaluator
    val evaluator = new MulticlassClassificationEvaluator().
      setLabelCol("Cover_Type").
      setPredictionCol("prediction")

    // lets get the classification metrics
    val accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
    val f1 = evaluator.setMetricName("f1").evaluate(predictions)
    println("\nPrinting classification metrics")
    println("accuracy: " + accuracy)
    println("f1: " + f1)

    // for getting the confusion matrix out we have multiclassmetrics available from mllib
    // which expects RDDs
    // we can always get the underlying RDD
    val predictionRDD = predictions.
      select("prediction", "Cover_Type").
      as[(Double, Double)].rdd
    val multiclassMetrics = new MulticlassMetrics(predictionRDD)
    println("/nPrinting the confusion matrix obtaned from multiclass metrics")

    println(multiclassMetrics.confusionMatrix)

    // we can use pivot functoin on dataframe to get our confusion matrix
    val confusionMatrix = predictions.
      groupBy("Cover_Type"). // the row items
      pivot("prediction", (1 to 7)). // the column items
      count(). // the pivot functoin
      na.fill(0.0). // fill 0 for nas
      orderBy("Cover_Type")

    println("\nPrinting the confusion matrix obtained directly by pivoting the predictins dataframe")
    confusionMatrix.show()
  }

  def classProbabilities(data: DataFrame): Array[Double] = {
    // to calcluate distribution of result classes in any part dataframe that we obtain using splits
    val total = data.count()
    data.groupBy("Cover_Type").count().
      orderBy("Cover_Type").
      select("count").as[Double].
      map(_ / total).
      collect()
  }

  def evaluate(trainData: DataFrame, testData: DataFrame): Unit = {

    val inputCols = trainData.columns.filter(_ != "Cover_Type")
    val assembler = new VectorAssembler().
      setInputCols(inputCols).
      setOutputCol("featureVector")

    val classifier = new DecisionTreeClassifier().
      setSeed(Random.nextLong()).
      setLabelCol("Cover_Type").
      setFeaturesCol("featureVector").
      setPredictionCol("prediction")

    val pipeline = new Pipeline().setStages(Array(assembler, classifier))

    val paramGrid = new ParamGridBuilder().
      addGrid(classifier.impurity, Seq("gini", "entropy")).
      addGrid(classifier.maxDepth, Seq(1, 20)).
      addGrid(classifier.maxBins, Seq(40, 300)).
      addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
      build()

    val multiclassEval = new MulticlassClassificationEvaluator().
      setLabelCol("Cover_Type").
      setPredictionCol("prediction").
      setMetricName("accuracy")

    // we employ the tra8h8hg validation split to run the pipeline
    // and setting train ratio we can hold out a cross vaildatoin set from within the training data
    val validator = new TrainValidationSplit().
      setSeed(Random.nextLong()).
      setEstimator(pipeline).
      setEvaluator(multiclassEval).
      setEstimatorParamMaps(paramGrid).
      setTrainRatio(0.9)

    // we fit it to the train data
    val validatorModel = validator.fit(trainData)

    val paramsAndMetrics = validatorModel.validationMetrics.
      zip(validatorModel.getEstimatorParamMaps).sortBy(-_._1)

    println("\nPriting the accuracy achieved for each parameter combination")
    paramsAndMetrics.foreach {
      case (metric, params) =>
        println(metric)
        println(params)
        println()
    }

    // get the best model
    val bestModel = validatorModel.bestModel

    println("\nPrinting the parameters that were employed to get the best model")
    println(bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap)

    println("Printing the max validation metrics achieved by any of the runs")
    println(validatorModel.validationMetrics.max)

    // find out the performance for the test data
    val testAccuracy = multiclassEval.evaluate(bestModel.transform(testData))
    println("\nAccuracy on the test data: " + testAccuracy)

    val trainAccuracy = multiclassEval.evaluate(bestModel.transform(trainData))
    println("\nAccuracy on the train data: " + trainAccuracy)
  }

  def unencodeOneHot(data: DataFrame): DataFrame = {
    val wildernessCols = (0 until 4).map(i => s"Wilderness_Area_$i").toArray

    val wildernessAssembler = new VectorAssembler().
      setInputCols(wildernessCols).
      setOutputCol("wilderness")
    // we have a udf here which is going to find the item which is 1.0 in the 4 columns
    // for wilderness and the 40 for the soil
    val unhotUDF = udf((vec: Vector) => vec.toArray.indexOf(1.0).toDouble)

    //here we employ it to drop the wilderness columns and replace with a single one
    val withWilderness = wildernessAssembler.transform(data).
      drop(wildernessCols: _*).
      withColumn("wilderness", unhotUDF($"wilderness"))

    val soilCols = (0 until 40).map(i => s"Soil_Type_$i").toArray

    val soilAssembler = new VectorAssembler().
      setInputCols(soilCols).
      setOutputCol("soil")

    soilAssembler.transform(withWilderness).
      drop(soilCols: _*).
      withColumn("soil", unhotUDF($"soil"))
  }

  // here we are following the same steps as in def evaluate
  def evaluateCategorical(trainData: DataFrame, testData: DataFrame): Unit = {
    val unencTrainData = unencodeOneHot(trainData)
    val unencTestData = unencodeOneHot(testData)

    val inputCols = unencTrainData.columns.filter(_ != "Cover_Type")
    val assembler = new VectorAssembler().
      setInputCols(inputCols).
      setOutputCol("featureVector")

    val indexer = new VectorIndexer().
      setMaxCategories(40).
      setInputCol("featureVector").
      setOutputCol("indexedVector")

    val classifier = new DecisionTreeClassifier().
      setSeed(Random.nextLong()).
      setLabelCol("Cover_Type").
      setFeaturesCol("indexedVector").
      setPredictionCol("prediction")

    val pipeline = new Pipeline().setStages(Array(assembler, indexer, classifier))

    val paramGrid = new ParamGridBuilder().
      addGrid(classifier.impurity, Seq("gini", "entropy")).
      addGrid(classifier.maxDepth, Seq(1, 20)).
      addGrid(classifier.maxBins, Seq(40, 300)).
      addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
      build()

    val multiclassEval = new MulticlassClassificationEvaluator().
      setLabelCol("Cover_Type").
      setPredictionCol("prediction").
      setMetricName("accuracy")

    val validator = new TrainValidationSplit().
      setSeed(Random.nextLong()).
      setEstimator(pipeline).
      setEvaluator(multiclassEval).
      setEstimatorParamMaps(paramGrid).
      setTrainRatio(0.9)

    val validatorModel = validator.fit(unencTrainData)

    val bestModel = validatorModel.bestModel

    println(bestModel.asInstanceOf[PipelineModel].stages.last.extractParamMap)

    val testAccuracy = multiclassEval.evaluate(bestModel.transform(unencTestData))
    println(testAccuracy)
  }

  def evaluateForest(trainData: DataFrame, testData: DataFrame): Unit = {
    val unencTrainData = unencodeOneHot(trainData)
    val unencTestData = unencodeOneHot(testData)

    val inputCols = unencTrainData.columns.filter(_ != "Cover_Type")
    val assembler = new VectorAssembler().
      setInputCols(inputCols).
      setOutputCol("featureVector")

    val indexer = new VectorIndexer().
      setMaxCategories(40).
      setInputCol("featureVector").
      setOutputCol("indexedVector")

    val classifier = new RandomForestClassifier().
      setSeed(Random.nextLong()).
      setLabelCol("Cover_Type").
      setFeaturesCol("indexedVector").
      setPredictionCol("prediction").
      setImpurity("entropy").
      setMaxDepth(20).
      setMaxBins(300)

    val pipeline = new Pipeline().setStages(Array(assembler, indexer, classifier))

    val paramGrid = new ParamGridBuilder().
      addGrid(classifier.minInfoGain, Seq(0.0, 0.05)).
      addGrid(classifier.numTrees, Seq(1, 10)).
      build()

    val multiclassEval = new MulticlassClassificationEvaluator().
      setLabelCol("Cover_Type").
      setPredictionCol("prediction").
      setMetricName("accuracy")

    val validator = new TrainValidationSplit().
      setSeed(Random.nextLong()).
      setEstimator(pipeline).
      setEvaluator(multiclassEval).
      setEstimatorParamMaps(paramGrid).
      setTrainRatio(0.9)

    val validatorModel = validator.fit(unencTrainData)

    val bestModel = validatorModel.bestModel

    val forestModel = bestModel.asInstanceOf[PipelineModel].
      stages.last.asInstanceOf[RandomForestClassificationModel]

    println("\nThe best forest model parameters")
    println(forestModel.extractParamMap)
    println("\nThe nubmer of trees used by the forestModel")
    println(forestModel.getNumTrees)
    println("\nFeature importances obtained from random forest")
    forestModel.featureImportances.toArray.zip(inputCols).
      sorted.reverse.foreach(println)

    val testAccuracy = multiclassEval.evaluate(bestModel.transform(unencTestData))
    println(testAccuracy)

    bestModel.transform(unencTestData.drop("Cover_Type")).select("prediction").show()
  }

}
