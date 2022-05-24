package hudi.issue.example

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.UUID

trait SparkHudiBase {

  protected val tempPath: String = s"${System.getProperty("user.dir")}/hudi-test/${UUID.randomUUID().toString}"

  protected val sparkConf: SparkConf = new SparkConf()
    .setAppName("Hudi Time Travel Issue")
    .setMaster("local[4]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  protected val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate

  protected def loadTimeline(): HoodieActiveTimeline = {
    HoodieTableMetaClient
      .builder()
      .setConf(new Configuration())
      .setBasePath(tempPath)
      .build()
      .getActiveTimeline
  }

  protected def writeToHudi(data: Seq[String]): Unit = {
    createDataFrame(data).write
      .format("hudi")
      .option(TABLE_TYPE.key(), "MERGE_ON_READ")
      .option(HoodieWriteConfig.TBL_NAME.key(), "test")
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "version")
      .option(PARTITIONPATH_FIELD.key(), "partition")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .option("hoodie.finalize.write.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(SaveMode.Append)
      .save(tempPath)
  }

  private def createDataFrame(data: Seq[String]): DataFrame = {
    import spark.implicits._
    val dataFrame = spark.read.json(data.toDS)
    dataFrame
  }

}
