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
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import window.ProcessWindowExample;

public class Main {

  private final static String INPUT_TOPIC = "input-topic";
  private final static String OUTPUT_TOPIC = "output-topic";
  private final static String JOB_TITLE = "FlinkWindow";
  private final static String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  private static final Time TUMBLING_WINDOW_TIME = Time.seconds(10);

  public static void main(String[] args) throws Exception {
    LOGGER.info("Starting main job ");
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
    //getting kafka Sink
    KafkaSink<String> kafkaSink = getStringKafkaSink(serializer);
    //Upper case of the input data
    WindowedStream<User, Integer, TimeWindow> tumblingWindowedStream =
        //Key by account number
        dataStream.keyBy(User::getAccountNumber)
            //Tumbling window
            .window(TumblingProcessingTimeWindows.of(TUMBLING_WINDOW_TIME));
    // Add the sink to so results
    // are written to the outputTopic
    tumblingWindowedStream.process(new ProcessWindowExample()).sinkTo(kafkaSink);
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
