package hudi.issue.example

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.model.{BaseAvroPayload, HoodieRecord, HoodieRecordPayload}
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig.{TBL_NAME, WRITE_PAYLOAD_CLASS_NAME}
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties

object HudiPrecombineFieldChange extends App with SparkHudiBase {

  // Use id as pre-combine field
  writeToHudiWithIdAsPrecombine(
    Seq(
      """{"id": 1, "ts": "1", "partition": "partition1", "value":{"x": 0}}""",
      """{"id": 2, "ts": "1", "partition": "partition1", "value":{"x": 1}}""",
    )
  )

  // Use ts as pre-combine field by providing another payload class implementation for pre-combine/merge process
  writeToHudiWithTsAsPrecombine(
    Seq(
      """{"id": 1, "ts": "1", "partition": "partition1", "value":{"x": 5}}""",
    )
  )

  val df: DataFrame = spark.read.format("hudi").load(s"$tempPath")
  df.show()

  protected def writeToHudiWithIdAsPrecombine(data: Seq[String]): Unit = {
    createDataFrame(data).write
      .format("hudi")
      .option(TABLE_TYPE.key(), "COPY_ON_WRITE")
      .option(TBL_NAME.key(), "test")
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "id")
      .option(PARTITIONPATH_FIELD.key(), "partition")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .option("hoodie.finalize.write.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .options(additionalHudiOptions)
      .mode(SaveMode.Append)
      .save(tempPath)
  }

  protected def writeToHudiWithTsAsPrecombine(data: Seq[String]): Unit = {
    createDataFrame(data).write
      .format("hudi")
      .option(TABLE_TYPE.key(), "COPY_ON_WRITE")
      .option(TBL_NAME.key(), "test")
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "id")
      .option(PARTITIONPATH_FIELD.key(), "partition")
      .option(WRITE_PAYLOAD_CLASS_NAME.key(), "hudi.issue.example.CustomPrecombinePayload")
      .option(PAYLOAD_CLASS_NAME.key(), "hudi.issue.example.CustomPrecombinePayload")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .option("hoodie.finalize.write.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .options(additionalHudiOptions)
      .mode(SaveMode.Append)
      .save(tempPath)
  }

}

class CustomPrecombinePayload(val record: GenericRecord, orderingVal: Comparable[_ <: Object]) extends
  BaseAvroPayload(record: GenericRecord, orderingVal: Comparable[_ <: Object])
  with HoodieRecordPayload[CustomPrecombinePayload] {

  override def preCombine(oldValue: CustomPrecombinePayload): CustomPrecombinePayload = {
    preCombine(oldValue, null)
  }

  override def preCombine(oldValue: CustomPrecombinePayload, properties: Properties): CustomPrecombinePayload = {
    if (record.get("ts").asInstanceOf[String] >= oldValue.record.get("ts").asInstanceOf[String])
      this
    else
      oldValue
  }

  override def getInsertValue(schema: Schema): Option[IndexedRecord] = {
    getInsertValue(schema, null)
  }

  override def getInsertValue(schema: Schema, properties: Properties): Option[IndexedRecord] = {
    if (recordBytes.length == 0) {
      return Option.empty()
    }
    val indexedRecord: IndexedRecord = HoodieAvroUtils.bytesToAvro(recordBytes, schema)
    if (isDeleteRecord(indexedRecord.asInstanceOf[GenericRecord])) {
      Option.empty()
    } else {
      Option.of(indexedRecord)
    }
  }

  def isDeleteRecord(genericRecord: GenericRecord): Boolean = {
    val isDeleteKey: String = HoodieRecord.HOODIE_IS_DELETED
    // Modify to be compatible with new version Avro.
    // The new version Avro throws for GenericRecord.get if the field name
    // does not exist in the schema.
    if (genericRecord.getSchema.getField(isDeleteKey) == null) {
      false
    } else {
      val deleteMarker: Any = genericRecord.get(isDeleteKey)
      deleteMarker.isInstanceOf[Boolean] && deleteMarker.asInstanceOf[Boolean]
    }
  }

  override def combineAndGetUpdateValue(currentValue: IndexedRecord, schema: Schema): Option[IndexedRecord] = {
    getInsertValue(schema)
  }
}

