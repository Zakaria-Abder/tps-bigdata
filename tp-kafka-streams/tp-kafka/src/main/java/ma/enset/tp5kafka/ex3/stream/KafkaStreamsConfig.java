package ma.enset.tp5kafka.ex3.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

  @Bean
  public KStream<String, String> kStream(StreamsBuilder streamsBuilder) {
    // 1
    KStream<String, String> clickStream = streamsBuilder.stream("clicks",
        Consumed.with(Serdes.String(), Serdes.String()));

    // 2
    KTable<String, Long> globalClickCountsTable = clickStream
        .selectKey((key, value) -> "totalClicks")
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .count(Materialized.as("global-clicks-store"));

    // 3
    globalClickCountsTable.toStream()
        .mapValues(Object::toString)
        .to("click-counts", Produced.with(Serdes.String(), Serdes.String()));

    return clickStream;
  }
}