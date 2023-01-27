package sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkMDsJSON {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkJSON")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val mlist = Array("""{"title": "Dangal", "country": "India", "year": 2016, "cast": {"Director": "Nitesh Tiwari", "LeadActor": "Aamir Khan", "BoxOfficeUSDMn": 330}, "genres": ["Biograhpy", "Drama"], "ratings": {"imdb": 8.4,"tomatoes": 4.55}}""", """{"title": "Fight Club", "country": "USA", "year": 1999, "cast": {"Director": "David Fincher", "LeadActor": "Brad Pitt", "BoxOfficeUSDMn": 104}, "genres": ["Action", "Drama"], "ratings": {"imdb": 8.8,"tomatoes": 4.46}}""")

    val mrdd = sc.parallelize(mlist)
    import spark.implicits._
    val movieJSONDF = spark.read.json(mrdd)
    movieJSONDF.show(false)

    //val cmplxSchema = StructType(Array(StructField("title", StringType, true), StructField("country", StringType, true), StructField("year", IntegerType, true), StructField("cast", StructType(Array(StructField("Director", StringType, true), StructField("LeadActor", StringType, true), StructField("BoxOfficeUSDMn", DoubleType, true))), true), StructField("genres", ArrayType(StringType, true), true), StructField("ratings", MapType(StringType, DoubleType))))
    // schema formatted with indentation to discern the embedded strucures more easily
    val cmplxSchema = StructType(
    Array(
    StructField("title", StringType, true), 
    StructField("country", StringType, true), 
    StructField("year", IntegerType, true), 
    StructField("cast", StructType(
        Array(
        StructField("Director", StringType, true), 
        StructField("LeadActor", StringType, true), 
        StructField("BoxOfficeUSDMn", DoubleType, true)
    )), true), 
    StructField("genres", ArrayType(StringType, true), true), 
    StructField("ratings", MapType(StringType, DoubleType))
    )
   )
    val movieJSONWithSchemaDF = spark.read.schema(cmplxSchema).json(mrdd)
    movieJSONWithSchemaDF.show(false)
    movieJSONWithSchemaDF.printSchema

    // querying complex types
    // arrays with indexing
    movieJSONWithSchemaDF.select($"genres"(0), $"genres"(1)).show
    // structs using dot notation
    movieJSONWithSchemaDF.select("cast.Director", "cast.LeadActor").show
    // maps using dot notations
    movieJSONWithSchemaDF.select("ratings.imdb", "ratings.tomatoes").show
    // maps using keys
    movieJSONWithSchemaDF.select($"ratings"("imdb"), $"ratings"("tomatoes")).show

    // filter using cmplex type part
    movieJSONWithSchemaDF.filter("cast.LeadActor = 'Aamir Khan'").show(false)

    // we can use the nested complex fields for grouping and aggregations
    movieJSONWithSchemaDF.groupBy($"cast.LeadActor")
      .agg(sum($"ratings.imdb") as "imdbtotal",
        sum($"ratings.tomatoes") as "tomtot").show

    // we can flatten the complex types using the explode function on arrays and maps
    import org.apache.spark.sql.functions.explode
    movieJSONWithSchemaDF.select(explode($"genres")).show
    movieJSONWithSchemaDF.select(explode($"genres"), $"*").show(false)

    // in one select clause we can have only one expansion. the line below will create an error
    // movieJSONWithSchemaDF.select(explode($"genres"), explode($"ratings"), $"*").show(false)

    // for multiple expansions we will have to do them sequentially
    movieJSONWithSchemaDF.select(explode($"genres"), $"*").select(explode($"ratings"), $"*").show(false)

    // sql api for querying complex types
    movieJSONWithSchemaDF.createOrReplaceTempView("mvcmplxtbl")
    // struct parts using dot notation and genres exploded
    spark.sql("select cast.director, cast.leadactor, ratings.imdb, ratings.tomatoes, explode(genres), *  from mvcmplxtbl").show(false)

    spark.sql("select cast.leadactor, sum(ratings.imdb) as imdbtot, sum(ratings.tomatoes) as tomtot from mvcmplxtbl group by cast.leadactor").show(false)

    // we will use the get_json_object, from_json and to_json functions here to work with rdds of json strings
    import org.apache.spark.sql.functions._
    // we create mjsonDF as a dataframe of strings
    val mjsonDF = sc.parallelize(mlist).toDF("mjson")

    // we can use get_json_object to navigate paths of a json string
    mjsonDF.select(get_json_object($"mjson", "$.cast")).show
    mjsonDF.select(get_json_object($"mjson", "$.cast.Director")).show

    // we can use from_json along with a schema to load json and then use dot notation to
    // access any path of the generated json
    mjsonDF.select(from_json($"mjson", cmplxSchema)).show(false)
    mjsonDF.select(from_json($"mjson", cmplxSchema) as "mdet").show(false)
    mjsonDF.select(from_json($"mjson", cmplxSchema) as "mdet").select("mdet.*").show(false)

    // finally we can call to_json to generate json data from the data frame
    // here we take the movieJSONWithSchemaDF we had created to get back the json strings from which we had created the dataframe
    movieJSONWithSchemaDF.select(to_json(struct($"*") ) as "moviestring").show(false)
  }
}
