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

package job;


import mapper.KafkaUserSchema;
import model.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import process.SimpleProcessFunction;

public class Main {

  private final static String INPUT_TOPIC = "input-topic";
  private final static String OUTPUT_TOPIC = "output-topic";
  private final static String JOB_TITLE = "FlinkWindow";
  private final static String BOOTSTRAP_SERVERS = "localhost:9092";

  public static void main(String[] args) throws Exception {

    // Set up the streaming execution environment
    final StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    //set parallelism
    streamExecutionEnvironment.setParallelism(4);

    //getting source
    KafkaSource<User> kafkaSource = getStringKafkaSource();

    DataStream<User> dataStream = streamExecutionEnvironment.fromSource(kafkaSource,
        WatermarkStrategy.noWatermarks(), "Source");

    //Serialization for kafka sink
    KafkaRecordSerializationSchema<String> serializer = getStringKafkaRecordSerializationSchema();

    //Sink
    KafkaSink<String> kafkaSink = getStringKafkaSink(serializer);

    //Upper case of the input data
    DataStream<String> counts =
        //Key by same input
        //dataStream.keyBy(st -> st)
        //attaching keyed by operator
        dataStream.process(new SimpleProcessFunction()).name("SimpleProcessFunction");

    // Add the sink to so results
    // are written to the outputTopic
    counts.sinkTo(kafkaSink);

    // Execute program
    streamExecutionEnvironment.execute(JOB_TITLE);
  }

  private static KafkaSink<String> getStringKafkaSink(
      KafkaRecordSerializationSchema<String> serializer) {
    return KafkaSink.<String>builder().setBootstrapServers(BOOTSTRAP_SERVERS)
        .setRecordSerializer(serializer).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
  }

  private static KafkaRecordSerializationSchema<String> getStringKafkaRecordSerializationSchema() {
    return KafkaRecordSerializationSchema.builder().setTopic(OUTPUT_TOPIC)
        .setValueSerializationSchema(new SimpleStringSchema()).build();
  }

  private static KafkaSource<User> getStringKafkaSource() {
    return KafkaSource.<User>builder().setBootstrapServers(BOOTSTRAP_SERVERS).setTopics(INPUT_TOPIC)
        .setGroupId("custom-group").setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new KafkaUserSchema()).build();
  }
}
