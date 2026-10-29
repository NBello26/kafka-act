package com.kafka.productor.Controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;


@RestController 
@RequestMapping ("/api/kafka")
public class ProductorController {
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired 
    public ProductorController(KafkaTemplate<String, String> kafkaTemplate){
        this.kafkaTemplate= kafkaTemplate;
    }

    @GetMapping("/send")
    public String getMethodName(
        @RequestParam String message,
        @RequestParam (required = false ) String key
    ) {
        String topicName= "tutorial-particiones";
        if(key!= null && !key.isEmpty()){
            kafkaTemplate.send(topicName, key, message);
            return "Mensaje enviado al topic: "+ topicName
            + "con key: "+ key
            + "mensaje: " + message;
        }else{
            kafkaTemplate.send(topicName, message);
              return "Mensaje enviado al topic: "+ topicName
            + "con key: sin key"
            + "mensaje: " + message;
        }
    }
    
}
