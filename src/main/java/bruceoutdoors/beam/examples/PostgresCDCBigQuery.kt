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

import avro.shaded.com.google.common.collect.ImmutableMap
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.options.*
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
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

    class avroToRowFn : DoFn<KV<ByteArray, ByteArray>, String>() {
        @ProcessElement
        fun processElement(@Element element: KV<ByteArray, ByteArray>, receiver: DoFn.OutputReceiver<String>) {
            receiver.output(element.key.toString())
        }
    }

    class avroToRow : PTransform<PCollection<KV<ByteArray, ByteArray>>, PCollection<String>>() {
        override fun expand(input: PCollection<KV<ByteArray, ByteArray>>): PCollection<String> {
            return input.apply(ParDo.of(avroToRowFn()))
        }
    }

    @Throws(IOException::class)
    @JvmStatic
    fun runWindowedWordCount(options: Options) {
        val output = options.output
        val p = Pipeline.create(options)

        p.apply("Read from Kafka",
                KafkaIO.read<ByteArray, ByteArray>()
                        .withBootstrapServers("localhost:9092")
                        .withTopic("dbserver1.inventory.customers")
                        .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", "earliest") as Map<String, Any>?)
                        .withoutMetadata()
        ).apply("2 Second Window",
                Window.into(FixedWindows.of(Duration.standardSeconds(WINDOW_SIZE)))
        ).apply("Avro to Row",
                avroToRow()
        )

        p.run().waitUntilFinish()
    }

    @Throws(IOException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val options = PipelineOptionsFactory.fromArgs(*args).withValidation().`as`(Options::class.java)
        runWindowedWordCount(options)
    }
}
