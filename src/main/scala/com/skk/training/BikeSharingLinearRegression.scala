package com.skk.training
import org.apache.spark.ml.feature.{ VectorAssembler, VectorIndexer }
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.TrainValidationSplit
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.regression.{ LinearRegression, LinearRegressionModel, GeneralizedLinearRegression }
import org.apache.spark.ml.{ Pipeline, PipelineModel }
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object BikeSharingLinearRegression extends App {
  val spark = SparkSession.builder
    .appName("LinRegression")
    .master("local[*]")
    .getOrCreate

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val df = spark.read.format("csv").option("header", "true").load(
    "D:/ufdata/bikesharing/hour.csv")

  df.registerTempTable("BikeSharing")
  print(df.count())
  spark.sql("SELECT * FROM BikeSharing").show()
  val df1 = df.drop("instant").drop("dteday").drop("casual").drop("registered")

  // convert to double: season,yr,mnth,hr,holiday,weekday,workingday,weathersit,
  // temp,atemp,hum,windspeed,casual,registered,cnt
  val df2 = df1.withColumn("season", df1("season").cast("double"))
    .withColumn("yr", df1("yr").cast("double"))
    .withColumn("mnth", df1("mnth").cast("double"))
    .withColumn("hr", df1("hr").cast("double"))
    .withColumn("holiday", df1("holiday").cast("double"))
    .withColumn("weekday", df1("weekday").cast("double"))
    .withColumn("workingday", df1("workingday").cast("double"))
    .withColumn("weathersit", df1("weathersit").cast("double"))
    .withColumn("temp", df1("temp").cast("double"))
    .withColumn("atemp", df1("atemp").cast("double"))
    .withColumn("hum", df1("hum").cast("double"))
    .withColumn("windspeed", df1("windspeed").cast("double"))
    .withColumn("label", df1("cnt").cast("double"))
    .drop("cnt")

  df2.printSchema()

  // lets check that the features are normalized
  // let us use rdd api to create a histogram and zeppelin visualization to get visual confidence
  val windHist = df2.select("windspeed").as[Double].rdd.histogram(10)
  val windHistTuples = (windHist._1.zip(windHist._2))
  val windHistRDD = spark.sparkContext.parallelize(windHistTuples)
  val windHistDF = windHistRDD.toDF("start", "count")

  // lets can the logic into a function
  def createHistogramDF(df: DataFrame, colname: String, buckets: Int): DataFrame = {
    val hist = df.select(colname).as[Double].rdd.histogram(buckets)
    val histTuples = (hist._1.zip(hist._2))
    val histRDD = df.rdd.sparkContext.parallelize(histTuples)
    val histDF = histRDD.toDF("start", "count")
    histDF
  }

  // drop label and create feature vector
  val df3 = df2.drop("label")
  val featureCols = df3.columns
  println(featureCols)
  val vectorAssembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("rawFeatures")

  val vectorIndexer = new VectorIndexer().setInputCol("rawFeatures").setOutputCol("features").setMaxCategories(10)

  val lr = new LinearRegression()
    .setFeaturesCol("features")
    .setLabelCol("label")
    .setRegParam(0.1)
    .setElasticNetParam(1.0)
    .setMaxIter(20)

  val pipeline = new Pipeline().setStages(Array(vectorAssembler, vectorIndexer, lr))

  val Array(training, test) = df2.randomSplit(Array(0.8, 0.2), seed = 12345)

  val model = pipeline.fit(training)

  val fullPredictions = model.transform(test).cache()
  val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
  val labels = fullPredictions.select("label").rdd.map(_.getDouble(0))
  val regMetrics = new RegressionMetrics(predictions.zip(labels))
  val RMSE = regMetrics.rootMeanSquaredError
  val r2COD = regMetrics.r2
  println(s"  Root mean squared error (RMSE): $RMSE")
  println(s" Coefficient fo determination (R2) : $r2COD")

  val lr_grid = new LinearRegression()
  val paramGrid = new ParamGridBuilder()
    .addGrid(lr_grid.regParam, Array(0.1, 0.01, 0.001))
    .addGrid(lr_grid.fitIntercept)
    .addGrid(lr_grid.elasticNetParam, Array(0.0, 0.25, 0.5, 0.75, 1.0))
    .addGrid(lr_grid.tol, Array(0.01, 0.001, 0.0001))
    .build()
  lr_grid.setMaxIter(1000)
  val pipelineGrid = new Pipeline().setStages(Array(vectorAssembler, vectorIndexer, lr_grid))

  val trainValidationSplit = new TrainValidationSplit()
    .setEstimator(pipelineGrid)
    .setEvaluator(new RegressionEvaluator)
    .setEstimatorParamMaps(paramGrid)
    .setTrainRatio(0.8)

  val linModelPipeline = trainValidationSplit.fit(training)
  val gridPredictions = linModelPipeline.transform(test).select("label", "prediction")
  val r2lmp = new RegressionMetrics(gridPredictions.rdd.map(x => (x.getDouble(1), x.getDouble(0))))

  println(r2lmp.rootMeanSquaredError)
  println(r2lmp.r2)

  val bestPipelineModel = linModelPipeline.bestModel.asInstanceOf[PipelineModel]
  val blinModel = bestPipelineModel.stages(2).asInstanceOf[org.apache.spark.ml.regression.LinearRegressionModel]
  blinModel.extractParamMap()

  blinModel.explainParams.foreach(println)

  val glr = new GeneralizedLinearRegression()
    .setFamily("gaussian")
    .setLink("identity")
    .setMaxIter(10)
    .setRegParam(0.3)

  val pipelineGLR = new Pipeline().setStages(Array(vectorAssembler, vectorIndexer, glr))

  val glrPipelineModel = pipelineGLR.fit(training)

  val glrModel = glrPipelineModel.stages(2).asInstanceOf[org.apache.spark.ml.regression.GeneralizedLinearRegressionModel]

  println("Coefficients of the glr model")
  print("\n" + glrModel.coefficients)

  val glrPredictions = glrPipelineModel.transform(test).select("label", "prediction")
  val glrRM = new RegressionMetrics(glrPredictions.rdd.map(x => (x.getDouble(1), x.getDouble(0))))

  println(glrRM.rootMeanSquaredError)
  println(glrRM.r2)
}
