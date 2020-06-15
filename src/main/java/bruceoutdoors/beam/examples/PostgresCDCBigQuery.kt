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

import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableReference
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.bigquery.model.TableSchema
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.options.*
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.joda.time.Duration
import java.io.IOException

object PostgresCDCBigQuery {
    const val WINDOW_SIZE: Long = 2

    interface Options : PipelineOptions, StreamingOptions {
        @get:Description("Confluent Schema Registry URL")
        @get:Default.String("http://localhost:8081")
        var schemaRegistry : String

        @get:Description("Path of the file to write to")
        var output: String?

        @get:Description("auto.offset.reset setting in kafka")
        @get:Default.String("earliest")
        var auto_offset_reset: String

        @get:Description("Bootstrap Servers")
        @get:Default.String("localhost:9092")
        var bootstrapServers: String
    }

    class AvroToRow : PTransform<PCollection<KV<ByteArray, ByteArray>>, PCollection<TableRow>>() {
        var avroDeserializer: KafkaAvroDeserializer? = null
        var schemaReg: ValueProvider<String>? = null

        override fun expand(input: PCollection<KV<ByteArray, ByteArray>>): PCollection<TableRow> {
            return input.apply(ParDo.of(object: DoFn<KV<ByteArray, ByteArray>, TableRow>() {
                @ProcessElement
                fun processElement(@DoFn.Element rec: KV<ByteArray, ByteArray>, out: DoFn.OutputReceiver<TableRow>) {
                    if (avroDeserializer == null) {
                        if (schemaReg == null) {
                            throw RuntimeException("Schema Registry must be defined!")
                        }

                        val schemaClient = CachedSchemaRegistryClient(schemaReg?.get(), 2147483647)
                        avroDeserializer = KafkaAvroDeserializer(schemaClient)
                    }

                    val decoded = avroDeserializer!!.deserialize("peanut", rec.value) as GenericRecord

                    out.output(TableRow()
                            .set("id", decoded.get("id") as Int)
                            .set("first_name", (decoded.get("first_name") as Utf8).toString())
                            .set("last_name", (decoded.get("last_name") as Utf8).toString())
                            .set("email", (decoded.get("email")  as Utf8).toString())
                            .set("__op", (decoded.get("__op") as Utf8).toString())
                            .set("__source_ts_ms", decoded.get("__source_ts_ms") as Long)
                            .set("__lsn", decoded.get("__lsn") as Long));
                }
            }))
        }

        fun setSchemaReg(schReg: String): AvroToRow {
            schemaReg = ValueProvider.StaticValueProvider.of(schReg) as ValueProvider<String>
            return this
        }
    }

    @Throws(IOException::class)
    @JvmStatic
    fun runPipeline(options: Options) {
        options.setStreaming(true)
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
                        .setName("last_name")
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

        var tableData = p.apply("Read from Kafka",
                KafkaIO.read<ByteArray, ByteArray>()
                        .withBootstrapServers(options.bootstrapServers)
                        .withTopic("dbserver1.inventory.customers")
                        .withConsumerConfigUpdates(ImmutableMap.of("auto.offset.reset", options.auto_offset_reset as Any))
                        // It's strange the below line does not work because DeserializerProvider is a private interface
//                        .withValueDeserializer(ConfluentSchemaRegistryDeserializerProvider.of("http://localhost:8081", "dbserver1.inventory.customers"))
                        .withKeyDeserializer(ByteArrayDeserializer::class.java)
                        .withValueDeserializer(ByteArrayDeserializer::class.java)
                        .withoutMetadata()
        ).apply("2 Second Window",
                Window.into<KV<ByteArray, ByteArray>>(FixedWindows.of(Duration.standardSeconds(WINDOW_SIZE)))
        ).apply("Avro to Row",
                AvroToRow().setSchemaReg(options.schemaRegistry)
        )

        if (options.output == null) {
            tableData.apply("Write to BigQuery",
                    BigQueryIO.writeTableRows()
                            .to(tableSpec)
                            .withSchema(tableSchema)
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            )
        } else {
            tableData.apply("Convert Rows To Pretty String",
                    MapElements.into(TypeDescriptor.of(String::class.java))
                            .via(ProcessFunction<TableRow, String> { input -> input.toPrettyString() })
            ).apply("Write To File (Testing Only)",
                    TextIO.write()
                            .withWindowedWrites()
                            .withNumShards(1)
                            .to(options.output)
            )
        }

        p.run().waitUntilFinish()
    }

    @Throws(IOException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val options = PipelineOptionsFactory.fromArgs(*args).withValidation().`as`(Options::class.java)
        runPipeline(options)
    }
}
