package hudi.issue.example

import org.apache.hudi.DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT
import org.apache.spark.sql.{DataFrame, Row}

object HudiTimeTravelExample extends App with SparkHudiBase {

  // Do an initial commit to Hudi
  writeToHudi(
    Seq(
      """{"id": 1, "version": "1", "partition": "partition1", "value":{"x": 0}}""",
      """{"id": 2, "version": "1", "partition": "partition1", "value":{"x": 0}}"""
    )
  )

  // Add one more commit to another base file
  writeToHudi(
    Seq(
      """{"id": 3, "version": "1", "partition": "partition1", "value":{"x": 1}}""",
      """{"id": 4, "version": "1", "partition": "partition1", "value":{"x": 1}}"""
    )
  )

  // Get first commit
  val firstCommit: String = loadTimeline().filterCompletedInstants().firstInstant().get().getTimestamp

  val df: DataFrame = spark
    .read
    .format("hudi")
    .option(TIME_TRAVEL_AS_OF_INSTANT.key(), firstCommit)
    // Read using path with asterisk - this seems to be a cause
    .load(s"$tempPath/*")

  df.show()

  assert(df.count() == 2, "Only the 1st commit should be returned")
}
