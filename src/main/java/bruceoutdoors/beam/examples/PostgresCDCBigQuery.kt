/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bruceoutdoors.beam.examples

import avro.shaded.com.google.common.collect.ImmutableList
import avro.shaded.com.google.common.collect.ImmutableMap
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.options.*
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.KV
import org.apache.kafka.common.serialization.Deserializer
import org.joda.time.Duration
import java.io.IOException


object PostgresCDCBigQuery {
    const val WINDOW_SIZE: Long = 2

    interface Options : PipelineOptions {
        @get:Description("Path of the file to read from")
        @get:Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        var inputFile: String

        @get:Description("Path of the file to write to")
        @get:Validation.Required
        var output: String
    }

    class AvroToRow : InferableFunction<KV<ByteArray, GenericRecord>, TableRow>() {
        override fun apply(record: KV<ByteArray, GenericRecord>): TableRow {
            return TableRow()
                    .set("id", record.value.get("id") as Long)
                    .set("first_name", record.value.get("first_name") as String)
                    .set("last_time", record.value.get("last_time") as String)
                    .set("email", record.value.get("email") as String)
                    .set("__op", record.value.get("__op") as String)
                    .set("__source_ts_ms", record.value.get("__source_ts_ms") as Long)
                    .set("__lsn", record.value.get("__lsn") as Long)
        }
    }

    @Throws(IOException::class)
    @JvmStatic
    fun runPipeline(options: Options) {
        val output = options.output
        val p = Pipeline.create(options)

        val tableSpec: TableReference = TableReference()
                .setProjectId("crafty-apex-264713")
                .setDatasetId("inventory")
                .setTableId("customers")

        val tableSchema: TableSchema = TableSchema().setFields(ImmutableList.of(
                TableFieldSchema()
                        .setName("id")
                        .setType("INT64"),
                TableFieldSchema()
                        .setName("first_name")
                        .setType("STRING"),
                TableFieldSchema()
                        .setName("last_time")
                        .setType("STRING"),
                TableFieldSchema()
                        .setName("email")
                        .setType("STRING"),
                TableFieldSchema()
                        .setName("__op")
                        .setType("STRING"),
                TableFieldSchema()
                        .setName("__source_ts_ms")
                        .setType("INT64"),
                TableFieldSchema()
                        .setName("__lsn")
                        .setType("INT64")
        ))


        p.apply("Read from Kafka",
                KafkaIO.read<ByteArray, GenericRecord>()
                        .withBootstrapServers("localhost:9092")
                        .withTopic("dbserver1.inventory.customers")
                        .withConsumerConfigUpdates(ImmutableMap.of("auto.offset.reset", "earliest" as Any))
                        .withConsumerConfigUpdates(ImmutableMap.of("specific.avro.reader", "true" as Any))
                        .withValueDeserializerAndCoder(KafkaAvroDeserializer::class.java as Class<out Deserializer<GenericRecord>>, AvroCoder.of(GenericRecord::class.java))
                        .withoutMetadata()
        ).apply("2 Second Window",
                Window.into<KV<ByteArray, GenericRecord>>(FixedWindows.of(Duration.standardSeconds(WINDOW_SIZE)))
        ).apply("Avro to Row",
                MapElements.via(AvroToRow())
        ).apply("Write to BigQuery",
                BigQueryIO.writeTableRows()
                        .to(tableSpec)
                        .withSchema(tableSchema)
                        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
        )

        p.run().waitUntilFinish()
    }

    @Throws(IOException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val options = PipelineOptionsFactory.fromArgs(*args).withValidation().`as`(Options::class.java)
        runPipeline(options)
    }
}
