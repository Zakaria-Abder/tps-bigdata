package ma.enset.tp5kafka.ex2;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WeatherApp {

  public static void main(String[] args) {
    // 1
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-analysis-app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

    StreamsBuilder builder = new StreamsBuilder();

    // 2
    KStream<String, String> sourceStream = builder.stream("weather-data");

    // 3
    KStream<String, String> parsedStream = sourceStream
        .mapValues(value -> {
          try {
            String[] parts = value.split(",");
            if (parts.length == 3) {
              String station = parts[0].trim();
              double tempC = Double.parseDouble(parts[1].trim());
              double humidity = Double.parseDouble(parts[2].trim());
              return station + "," + tempC + "," + humidity;
            }
          } catch (Exception e) {
          }
          return null;
        })
        .filter((key, value) -> value != null)

        // Filtrer Température > 30°C
        .filter((key, value) -> {
          double tempC = Double.parseDouble(value.split(",")[1]);
          return tempC > 30.0;
        })

        // Convertir en Fahrenheit (F = C * 9/5 + 32)
        .mapValues(value -> {
          String[] parts = value.split(",");
          String station = parts[0];
          double tempC = Double.parseDouble(parts[1]);
          double humidity = Double.parseDouble(parts[2]);

          double tempF = (tempC * 9.0 / 5.0) + 32.0;
          return station + "," + tempF + "," + humidity;
        })

        // définir la Station comme Clé pour le regroupement
        .selectKey((key, value) -> value.split(",")[0]);

    // 4
    KTable<String, String> stationAverages = parsedStream
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .aggregate(
            () -> "0,0.0,0.0", // valeur initiale (Count, SumTemp, SumHumidity)
            (stationKey, newValue, aggValue) -> {
              String[] newParts = newValue.split(",");
              double tempF = Double.parseDouble(newParts[1]);
              double hum = Double.parseDouble(newParts[2]);

              String[] aggParts = aggValue.split(",");
              int count = Integer.parseInt(aggParts[0]) + 1;
              double sumTemp = Double.parseDouble(aggParts[1]) + tempF;
              double sumHum = Double.parseDouble(aggParts[2]) + hum;

              return count + "," + sumTemp + "," + sumHum;
            },
            Materialized.with(Serdes.String(), Serdes.String())
        );

    // 5
    stationAverages.toStream()
        .mapValues((stationKey, aggValue) -> {
          String[] parts = aggValue.split(",");
          int count = Integer.parseInt(parts[0]);
          double sumTemp = Double.parseDouble(parts[1]);
          double sumHum = Double.parseDouble(parts[2]);

          double avgTemp = sumTemp / count;
          double avgHum = sumHum / count;

          return String.format("%s : Temperature moyenne = %.1f F, Humidite moyenne = %.1f %%",
              stationKey, avgTemp, avgHum);
        })
        // 6
        .to("station-averages", Produced.with(Serdes.String(), Serdes.String()));

    // 7
    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    final CountDownLatch latch = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      streams.close();
      latch.countDown();
    }));

    try {
      streams.start();
      System.out.println("Weather Analysis App is running...");
      latch.await();
    } catch (InterruptedException e) {
      System.exit(1);
    }
  }
}