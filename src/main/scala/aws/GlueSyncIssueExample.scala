package hudi.issue.example
package aws

object GlueSyncIssueExample extends App with SparkHudiBase {

  // Set options to enable Hive sync, main Hudi options are in SparkHudiBase class
  override def additionalHudiOptions: Map[String, String] = {
    Map(
      "hoodie.datasource.hive_sync.partition_fields" -> "partition",
      "hoodie.datasource.hive_sync.mode" -> "hms",
      "hoodie.datasource.hive_sync.enable" -> "true",
      "hoodie.datasource.hive_sync.database" -> "glue-sync-issue-example-database",
      "hoodie.datasource.hive_sync.table" -> "test_table"
    )
  }

  // Do a simple write to Hudi and trigger Hive sync
  writeToHudi(
    Seq(
      """{"id": 1, "version": "1", "partition": "partition1", "value":{"x": 0}}""",
      """{"id": 2, "version": "1", "partition": "partition1", "value":{"x": 0}}"""
    )
  )

}
