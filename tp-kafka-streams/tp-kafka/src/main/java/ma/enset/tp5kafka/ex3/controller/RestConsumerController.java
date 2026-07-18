package ma.enset.tp5kafka.ex3.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RestController
@RequestMapping("/clicks")
public class RestConsumerController {

  private final Map<String, Integer> localClickStore = new ConcurrentHashMap<>();

  @KafkaListener(topics = "click-counts", groupId = "rest-consumer-group")
  public void consumeCounts(ConsumerRecord<String, String> record) {
    if (record.key() != null && record.value() != null) {
      localClickStore.put(record.key(), Integer.parseInt(record.value()));
    }
  }

  @GetMapping("/count")
  public Map<String, Integer> getClickCount() {
    return localClickStore;
  }
}