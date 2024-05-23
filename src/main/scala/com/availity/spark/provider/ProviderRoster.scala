import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object ProviderVisits {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ProviderVisits")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Read the providers.csv file
    val providers = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .csv("data/providers.csv")

    // Read the visits.csv file
    val visits = spark.read
      .option("header", "false")
      .csv("data/visits.csv")
      .toDF("visit_id", "visit_provider_id", "date_of_service")

    // Convert date_of_service to date type
    val visitsWithDate = visits.withColumn("date_of_service", to_date($"date_of_service"))

    // Task 1: Calculate the total number of visits per provider
    val totalVisitsPerProvider = visitsWithDate
      .groupBy("visit_provider_id")
      .agg(count("visit_id").alias("total_visits"))

    val providersWithVisits = providers
      .join(totalVisitsPerProvider, providers("provider_id") === totalVisitsPerProvider("visit_provider_id"))
      .select(
        providers("provider_id"),
        concat_ws(" ", $"first_name", $"middle_name", $"last_name").alias("provider_name"),
        $"provider_specialty",
        $"total_visits"
      )

    // Write the result partitioned by provider's specialty
    providersWithVisits
      .repartition($"provider_specialty")
      .write
      .partitionBy("provider_specialty")
      .json("output/total_visits_per_provider")

    // Task 2: Calculate the total number of visits per provider per month
    val visitsWithMonth = visitsWithDate
      .withColumn("month", date_format($"date_of_service", "yyyy-MM"))

    val totalVisitsPerProviderPerMonth = visitsWithMonth
      .groupBy("visit_provider_id", "month")
      .agg(count("visit_id").alias("total_visits"))
      .orderBy("visit_provider_id", "month")

    // Write the result as JSON
    totalVisitsPerProviderPerMonth
      .write
      .json("output/total_visits_per_provider_per_month")

    spark.stop()
  }
}
