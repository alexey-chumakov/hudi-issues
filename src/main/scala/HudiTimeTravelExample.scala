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

  //+-------------------+--------------------+------------------+----------------------+--------------------+---+----------+-----+-------+
  //|_hoodie_commit_time|_hoodie_commit_seqno|_hoodie_record_key|_hoodie_partition_path|   _hoodie_file_name| id| partition|value|version|
  //+-------------------+--------------------+------------------+----------------------+--------------------+---+----------+-----+-------+
  //|     20220526114900|  20220526114900_0_1|                 1|  partition=partition1|7f31f42a-a1d7-45d...|  1|partition1|  {0}|      1|
  //|     20220526114900|  20220526114900_0_2|                 2|  partition=partition1|7f31f42a-a1d7-45d...|  2|partition1|  {0}|      1|
  //|     20220526114905|  20220526114905_0_3|                 3|  partition=partition1|7f31f42a-a1d7-45d...|  3|partition1|  {1}|      1|
  //|     20220526114905|  20220526114905_0_4|                 4|  partition=partition1|7f31f42a-a1d7-45d...|  4|partition1|  {1}|      1|
  //+-------------------+--------------------+------------------+----------------------+--------------------+---+----------+-----+-------+

  assert(df.count() == 2, "Only the 1st commit should be returned")

}
