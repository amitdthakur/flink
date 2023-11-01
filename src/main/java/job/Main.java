package job;


import deserializer.CustomDeserializer;
import model.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import serializer.CustomSerializer;
import window.ProcessWindowExample;

public class Main {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  private final static String INPUT_TOPIC = "input-topic";
  private final static String OUTPUT_TOPIC = "output-topic";
  private final static String JOB_NAME = "Flink_Window_Job";
  private final static String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final Time TUMBLING_WINDOW_TIME = Time.seconds(10);

  public static void main(String[] args) throws Exception {
    LOGGER.info("Starting main job");
    Configuration configuration = new Configuration();
    //Set rest end point for flink UI
    configuration.setInteger(RestOptions.PORT, 8082);
    // Set up the streaming execution environment
    final StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(
        configuration);
    //set parallelism
    streamExecutionEnvironment.setParallelism(4);
    //getting source
    KafkaSource<User> kafkaSource = getStringKafkaSource();
    DataStream<User> dataStream = streamExecutionEnvironment.fromSource(kafkaSource,
        WatermarkStrategy.noWatermarks(), "KafkaSource");
    //getting kafka Sink
    KafkaSink<User> kafkaSink = getKafkaSink();
    //Tumbling window based on the time.
    WindowedStream<User, Integer, TimeWindow> tumblingWindowedStream =
        //Key by account number
        dataStream.keyBy(User::getAccountNumber)
            //Tumbling window
            .window(TumblingProcessingTimeWindows.of(TUMBLING_WINDOW_TIME));
    // Add the sink to so results are written to the outputTopic
    SingleOutputStreamOperator<User> tumblingWindowStream = tumblingWindowedStream.process(
        new ProcessWindowExample());
    tumblingWindowStream.sinkTo(kafkaSink);
    // Execute program
    streamExecutionEnvironment.execute(JOB_NAME);
  }

  private static KafkaSink<User> getKafkaSink() {
    KafkaRecordSerializationSchema<User> serializer = getStringKafkaRecordSerializationSchema();
    return KafkaSink.<User>builder()
        .setBootstrapServers(BOOTSTRAP_SERVERS)
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .setRecordSerializer(serializer)
        .build();
  }

  private static KafkaRecordSerializationSchema<User> getStringKafkaRecordSerializationSchema() {
    //Serialization for kafka sink
    return KafkaRecordSerializationSchema.builder()
        .setTopic(OUTPUT_TOPIC)
        .setValueSerializationSchema(new CustomSerializer())
        .build();
  }

  private static KafkaSource<User> getStringKafkaSource() {
    return KafkaSource.<User>builder().setBootstrapServers(BOOTSTRAP_SERVERS)
        //Set the topic name
        .setTopics(INPUT_TOPIC)
        //set thr group ID
        .setGroupId("custom-group")
        //Starting offset
        .setStartingOffsets(OffsetsInitializer.latest())
        //Setting KafkaUserDeserializerSchema
        .setValueOnlyDeserializer(new CustomDeserializer()).build();
  }
}
