package hudi.issue.example

import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL
import org.apache.spark.sql.DataFrame

object HudiTimeTravelIssue extends App with SparkHudiBase {

  override def additionalHudiOptions: Map[String, String] = {
    Map(
      // Using KEEP_LATEST_COMMITS cleaner strategy for shorter test,
      // but the issue seems to be the same for all cleaner strategies
      "hoodie.cleaner.policy" -> "KEEP_LATEST_COMMITS",
      "hoodie.cleaner.commits.retained" -> "1",
      "hoodie.cleaner.hours.retained" -> "1",
    )
  }

  // Writing to partition 1 & 2 and taking the last commit time
  writeToHudi(
    Seq(
      """{"id": 1, "version": "1", "partition": "partition1", "value":{"x": 0}}""",
      """{"id": 2, "version": "1", "partition": "partition1", "value":{"x": 0}}""",
    )
  )
  writeToHudi(
    Seq(
      """{"id": 3, "version": "2", "partition": "partition2", "value":{"x": 0}}""",
      """{"id": 4, "version": "2", "partition": "partition2", "value":{"x": 0}}"""
    )
  )
  val timestamp: String = loadTimeline().lastInstant().get().getTimestamp

  // Writing to partition 2 once again to ensure cleaner is triggered
  writeToHudi(
    Seq(
      """{"id": 3, "version": "2", "partition": "partition2", "value":{"x": 0}}""",
      """{"id": 4, "version": "2", "partition": "partition2", "value":{"x": 0}}"""
    )
  )
  writeToHudi(
    Seq(
      """{"id": 3, "version": "2", "partition": "partition2", "value":{"x": 0}}""",
      """{"id": 4, "version": "2", "partition": "partition2", "value":{"x": 0}}"""
    )
  )

  // Here we read the data using time-travel query and expect records from partition 1 and 2
  private val df: DataFrame = spark.read.format("hudi")
    .option(DataSourceReadOptions.QUERY_TYPE.key(), QUERY_TYPE_SNAPSHOT_OPT_VAL)
    .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key(), timestamp)
    .load(tempPath)

  // I'm expecting 4 records, but there are only 2 from partition 1
  df.show()
  // +-------------------+--------------------+------------------+----------------------+--------------------+---+----------+-----+-------+
  // |_hoodie_commit_time|_hoodie_commit_seqno|_hoodie_record_key|_hoodie_partition_path|   _hoodie_file_name| id| partition|value|version|
  // +-------------------+--------------------+------------------+----------------------+--------------------+---+----------+-----+-------+
  // |  20230524184124881|20230524184124881...|                 1|  partition=partition1|baef0e39-e178-4f0...|  1|partition1|  {0}|      1|
  // |  20230524184124881|20230524184124881...|                 2|  partition=partition1|baef0e39-e178-4f0...|  2|partition1|  {0}|      1|
  // +-------------------+--------------------+------------------+----------------------+--------------------+---+----------+-----+-------+

}
