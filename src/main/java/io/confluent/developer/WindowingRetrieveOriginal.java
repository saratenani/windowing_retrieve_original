package io.confluent.developer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.confluent.demo.Inputdatat;
import io.confluent.demo.Anomaly;
import io.confluent.demo.Joined;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.lang.Integer.parseInt;
import static java.lang.Short.parseShort;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.common.serialization.Serdes.Double;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.streams.kstream.Grouped.with;

public class WindowingRetrieveOriginal {

  //region buildStreamsProperties
  protected Properties buildStreamsProperties(Properties envProps) {
    Properties config = new Properties();
    config.putAll(envProps);

    config.put(APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
    config.put(BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
    config.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    //config.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Double().getClass());
    config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));

    config.put(REPLICATION_FACTOR_CONFIG, envProps.getProperty("default.topic.replication.factor"));
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, envProps.getProperty("offset.reset.policy"));

    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    return config;
  }
  //endregion

  //region createTopics

  /**
   * Create topics using AdminClient API
   */
  private void createTopics(Properties envProps) {
    Map<String, Object> config = new HashMap<>();

    config.put("bootstrap.servers", envProps.getProperty("bootstrap.servers"));
    AdminClient client = AdminClient.create(config);

    List<NewTopic> topics = new ArrayList<>();

    topics.add(new NewTopic(
        envProps.getProperty("input1.topic.name"),
        parseInt(envProps.getProperty("input1.topic.partitions")),
        parseShort(envProps.getProperty("input1.topic.replication.factor"))));

    topics.add(new NewTopic(
            envProps.getProperty("input2.topic.name"),
            parseInt(envProps.getProperty("input2.topic.partitions")),
            parseShort(envProps.getProperty("input2.topic.replication.factor"))));

    topics.add(new NewTopic(
        envProps.getProperty("output.topic.name"),
        parseInt(envProps.getProperty("output.topic.partitions")),
        parseShort(envProps.getProperty("output.topic.replication.factor"))));

    topics.add(new NewTopic(
            "join_topic",
            parseInt(envProps.getProperty("output.topic.partitions")),
            parseShort(envProps.getProperty("output.topic.replication.factor"))));

    client.createTopics(topics);
    client.close();

  }
  //endregion

  private void run(String confFile) throws Exception {

    Properties envProps = this.loadEnvProperties(confFile);
    Properties streamProps = this.buildStreamsProperties(envProps);
    Topology topology = this.buildTopology(new StreamsBuilder(), envProps);

    this.createTopics(envProps);

    final KafkaStreams streams = new KafkaStreams(topology, streamProps);
    final CountDownLatch latch = new CountDownLatch(1);

    // Attach shutdown handler to catch Control-C.
    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        streams.close(Duration.ofSeconds(5));
        latch.countDown();
      }
    });

    try {
      streams.cleanUp();
      streams.start();
      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }
    System.exit(0);
  }

  //region buildTopology
  private Topology buildTopology(StreamsBuilder bldr,
                                 Properties envProps) {

    final String originalTopicName = envProps.getProperty("input1.topic.name");
    final String anomalyTopicName = envProps.getProperty("input2.topic.name");
    final String outputTopicName = envProps.getProperty("output.topic.name");

    final SpecificAvroSerde<Inputdatat> inputdataSerde = getInputdataSerde(envProps);
    final SpecificAvroSerde<Anomaly> anomalySerde = getAnomalySerde(envProps);
    final SpecificAvroSerde<Joined> joinedSerde = getJoinedSerde(envProps);
    final DataJoiner joiner = new DataJoiner();

    final KStream<String,Inputdatat> originalStream = bldr.stream(originalTopicName,Consumed.with(Serdes.String(), inputdataSerde))
            .selectKey((key, value) -> value.getKey());

    final KStream<String,Anomaly> anomalyStream = bldr.stream(anomalyTopicName,Consumed.with(Serdes.String(), anomalySerde));



    final KStream<String,Joined> joinedStream = originalStream.join(anomalyStream,
                                                                    joiner,
                                                                    JoinWindows.of(Duration.ofMillis(5000)),
                                                                    StreamJoined.with(Serdes.String(),inputdataSerde,anomalySerde));

    joinedStream.to("join_topic", Produced.with(Serdes.String(),joinedSerde));

   final KStream<String,Joined> filteredStream = joinedStream
                                                .filter((key, value) -> {
                                                  try {
                                                    return inWindow(value.getTimestamp(),value.getWindowStart(),value.getWindowEnd());
                                                  } catch (Exception e) {
                                                      e.printStackTrace();
                                                    }
                                                    return false;
                                                  });

    filteredStream
            .mapValues((value) -> Inputdatat.newBuilder()
                .setKey(value.getKey())
                .setTimestamp(value.getTimestamp())
                .setVal(value.getVal())
                .build())
            .to(outputTopicName, Produced.with(Serdes.String(),getInputdataSerde(envProps)));

    // finish the topology
    return bldr.build();
  }
  //endregion

  public static SpecificAvroSerde<Anomaly> getAnomalySerde(Properties envProps) {
    SpecificAvroSerde<Anomaly> idsSerde = new SpecificAvroSerde<>();

    final HashMap<String, String> serdeConfig = new HashMap<>();
    serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG,
            envProps.getProperty("schema.registry.url"));

    idsSerde.configure(serdeConfig, false);
    return idsSerde;

  }

  public static SpecificAvroSerde<Inputdatat> getInputdataSerde(Properties envProps) {
    SpecificAvroSerde<Inputdatat> inputdataAvroSerde = new SpecificAvroSerde<>();

    final HashMap<String, String> serdeConfig = new HashMap<>();
    serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG,
            envProps.getProperty("schema.registry.url"));

    inputdataAvroSerde.configure(serdeConfig, false);
    return inputdataAvroSerde;
  }

  public static SpecificAvroSerde<Joined> getJoinedSerde(Properties envProps) {
    SpecificAvroSerde<Joined> joinedAvroSerde = new SpecificAvroSerde<>();

    final HashMap<String, String> serdeConfig = new HashMap<>();
    serdeConfig.put(SCHEMA_REGISTRY_URL_CONFIG,
            envProps.getProperty("schema.registry.url"));

    joinedAvroSerde.configure(serdeConfig, false);
    return joinedAvroSerde;
  }

  protected static Map<String, String> getSerdeConfig(Properties config) {
    final HashMap<String, String> map = new HashMap<>();

    final String srUrlConfig = config.getProperty(SCHEMA_REGISTRY_URL_CONFIG);
    map.put(SCHEMA_REGISTRY_URL_CONFIG, ofNullable(srUrlConfig).orElse(""));
    return map;
  }

  private Properties loadEnvProperties(String fileName) throws IOException {
        Properties envProps = new Properties();
        FileInputStream input = new FileInputStream(fileName);
        envProps.load(input);
        input.close();

        return envProps;
    }

  private static boolean inWindow(String timetstamp, String start, String end) throws Exception{

    Date timestamp_d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(timetstamp);
    Date start_d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(start);
    Date end_d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(end);

    if(!start_d.after(timestamp_d) && end_d.after(timestamp_d))
      return true;

    return false;
  }

  public static void main(String[] args)throws Exception {
	if (args.length < 1) {
          throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }
    new WindowingRetrieveOriginal().run(args[0]);
  }
}
