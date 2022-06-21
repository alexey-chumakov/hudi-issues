package hudi.issue.example

import org.apache.spark.sql.DataFrame

object HudiEvolutionIssueExample extends App with SparkHudiBase {

  // Do an initial commit to Hudi (using nullable schema)
  writeToHudi(
    Seq(
      """{"id": 1, "version": "1", "partition": "partition1", "value":{"x": 0}}""",
      """{"id": 2, "version": "1", "partition": "partition1", "value":{"x": 0}}"""
    )
  )

  // Add one more commit to another base file
  writeToHudi(
    Seq(
      """{"id": 3, "version": "1", "partition": "partition1", "value":{"x": 1}, "value2": "abc"}""",
      """{"id": 4, "version": "1", "partition": "partition1", "value":{"x": 1}, "value2": "abc"}"""
    )
  )

  // Add commit to another partition
  writeToHudi(
    Seq(
      """{"id": 5, "version": "1", "partition": "partition2", "value":{"x": 1}, "value2": "abc", "value3": "xyz"}""",
      """{"id": 6, "version": "1", "partition": "partition2", "value":{"x": 1}, "value2": "abc", "value3": "xyz"}"""
    )
  )

  // Rewrite

  // Do an initial commit to Hudi (using nullable schema)
  writeToHudi(
    Seq(
      """{"id": 1, "version": "2", "partition": "partition1", "value":{"x": 0}}""",
      """{"id": 2, "version": "2", "partition": "partition1", "value":{"x": 0}}"""
    )
  )

  // Add one more commit to another base file
  writeToHudi(
    Seq(
      """{"id": 3, "version": "2", "partition": "partition1", "value":{"x": 1}, "value2": "abc"}""",
      """{"id": 4, "version": "2", "partition": "partition1", "value":{"x": 1}, "value2": "abc"}"""
    )
  )

  // Add commit to another partition
  writeToHudi(
    Seq(
      """{"id": 5, "version": "2", "partition": "partition2", "value":{"x": 1}, "value2": "abc", "value3": "xyz"}""",
      """{"id": 6, "version": "2", "partition": "partition2", "value":{"x": 1}, "value2": "abc", "value3": "xyz"}"""
    )
  )

  val df: DataFrame = spark
    .read
    .format("hudi")
    .load(s"$tempPath")

  df.printSchema()
  df.show()

  // For 0.9.0 version and Spark 3.1.2 - works correctly
  // +-------------------+--------------------+------------------+----------------------+--------------------+---+----------+-----+------+------+-------+
  //|_hoodie_commit_time|_hoodie_commit_seqno|_hoodie_record_key|_hoodie_partition_path|   _hoodie_file_name| id| partition|value|value2|value3|version|
  //+-------------------+--------------------+------------------+----------------------+--------------------+---+----------+-----+------+------+-------+
  //|  20220613162052236|20220613162052236...|                 1|  partition=partition1|99eea3e6-888f-455...|  1|partition1|  {0}|  null|  null|      1|
  //|  20220613162052236|20220613162052236...|                 2|  partition=partition1|99eea3e6-888f-455...|  2|partition1|  {0}|  null|  null|      1|
  //|  20220613162057371|20220613162057371...|                 3|  partition=partition1|99eea3e6-888f-455...|  3|partition1|  {1}|   abc|  null|      1|
  //|  20220613162057371|20220613162057371...|                 4|  partition=partition1|99eea3e6-888f-455...|  4|partition1|  {1}|   abc|  null|      1|
  //|  20220613162100326|20220613162100326...|                 6|  partition=partition2|7e29849f-6f98-481...|  6|partition2|  {1}|   abc|   xyz|      1|
  //|  20220613162100326|20220613162100326...|                 5|  partition=partition2|7e29849f-6f98-481...|  5|partition2|  {1}|   abc|   xyz|      1|
  //+-------------------+--------------------+------------------+----------------------+--------------------+---+----------+-----+------+------+-------+

  // For Hudi 0.10.0 and Spark 3.1.2 - works correctly
  //+-------------------+--------------------+------------------+----------------------+--------------------+---+----------+-----+------+------+-------+
  //|_hoodie_commit_time|_hoodie_commit_seqno|_hoodie_record_key|_hoodie_partition_path|   _hoodie_file_name| id| partition|value|value2|value3|version|
  //+-------------------+--------------------+------------------+----------------------+--------------------+---+----------+-----+------+------+-------+
  //|  20220613161809899|20220613161809899...|                 1|  partition=partition1|808370b0-74f5-401...|  1|partition1|  {0}|  null|  null|      1|
  //|  20220613161809899|20220613161809899...|                 2|  partition=partition1|808370b0-74f5-401...|  2|partition1|  {0}|  null|  null|      1|
  //|  20220613161815368|20220613161815368...|                 3|  partition=partition1|808370b0-74f5-401...|  3|partition1|  {1}|   abc|  null|      1|
  //|  20220613161815368|20220613161815368...|                 4|  partition=partition1|808370b0-74f5-401...|  4|partition1|  {1}|   abc|  null|      1|
  //|  20220613161819400|20220613161819400...|                 6|  partition=partition2|dc55f779-ffd3-477...|  6|partition2|  {1}|   abc|   xyz|      1|
  //|  20220613161819400|20220613161819400...|                 5|  partition=partition2|dc55f779-ffd3-477...|  5|partition2|  {1}|   abc|   xyz|      1|
  //+-------------------+--------------------+------------------+----------------------+--------------------+---+----------+-----+------+------+-------+

  // For Hudi 0.11.0 and Spark 3.2.0 - works correctly
  //+-------------------+--------------------+------------------+----------------------+--------------------+---+----------+-----+------+------+-------+
  //|_hoodie_commit_time|_hoodie_commit_seqno|_hoodie_record_key|_hoodie_partition_path|   _hoodie_file_name| id| partition|value|value2|value3|version|
  //+-------------------+--------------------+------------------+----------------------+--------------------+---+----------+-----+------+------+-------+
  //|  20220613132603570|20220613132603570...|                 6|  partition=partition2|bf81ca4c-930f-43f...|  6|partition2|  {1}|   abc|   xyz|      1|
  //|  20220613132603570|20220613132603570...|                 5|  partition=partition2|bf81ca4c-930f-43f...|  5|partition2|  {1}|   abc|   xyz|      1|
  //|  20220613132536222|20220613132536222...|                 1|  partition=partition1|045608cb-49a7-457...|  1|partition1|  {0}|  null|  null|      1|
  //|  20220613132536222|20220613132536222...|                 2|  partition=partition1|045608cb-49a7-457...|  2|partition1|  {0}|  null|  null|      1|
  //|  20220613132556746|20220613132556746...|                 3|  partition=partition1|045608cb-49a7-457...|  3|partition1|  {1}|   abc|  null|      1|
  //|  20220613132556746|20220613132556746...|                 4|  partition=partition1|045608cb-49a7-457...|  4|partition1|  {1}|   abc|  null|      1|
  //+-------------------+--------------------+------------------+----------------------+--------------------+---+----------+-----+------+------+-------+

}
