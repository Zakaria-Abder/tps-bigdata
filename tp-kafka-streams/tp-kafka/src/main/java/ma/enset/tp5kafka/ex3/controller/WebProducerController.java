package ma.enset.tp5kafka.ex3.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class WebProducerController {

  private final KafkaTemplate<String, String> kafkaTemplate;

  public WebProducerController(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @GetMapping("/")
  public String index() {
    return "index";
  }

  @PostMapping("/click")
  @ResponseBody
  public String registerClick(@RequestParam String userId) {
    kafkaTemplate.send("clicks", userId, "click");
    return "Click registered for " + userId;
  }
}