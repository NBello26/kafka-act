package com.kafka.productor.Controller;

import java.util.Random;
import java.util.UUID;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.productor.model.TelemetriaCamion;

@RestController 
@RequestMapping("/api/kafka")
public class ProductorController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // Inyección de dependencias por constructor
    public ProductorController(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @PostMapping("/send")
    public String postMethodName(
        @RequestBody String message,
        @RequestParam(required = false) String key 
    ) {
        // Corrección: Eliminados espacios innecesarios y erratas en el nombre del tópico
        String topicName = "telemetria-camion"; 
        
        if (key != null && !key.isEmpty()) {
            kafkaTemplate.send(topicName, key, message);
            return "Mensaje JSON enviado al topic: " + topicName 
                + " | con key: " + key 
                + " | mensaje: " + message;
        } else {
            kafkaTemplate.send(topicName, message);
            return "Mensaje JSON enviado al topic: " + topicName 
                + " | con key: sin key" 
                + " | mensaje: " + message;
        }
    }

    @PostMapping("/simular-camiones")
    public String simular50Mensajes() {
        String topicName = "telemetria-camion";
        String[] camiones = {"CAMION-001", "CAMION-002", "CAMION-003"};
        String[] estados = {"EN_RUTA", "DETENIDO", "REFRIGERANDO", "ALERTA_TEMPERATURA"};
        Random random = new Random();

        try {
            for (int i = 1; i <= 50; i++) {
                String camionId = camiones[random.nextInt(camiones.length)];

                double temperatura = -25.0 + (random.nextDouble() * 15.0);
                double humedad = 50.0 + (random.nextDouble() * 30.0);       
                String estado = estados[random.nextInt(estados.length)];
                long timestamp = System.currentTimeMillis() + (i * 1000L);

                TelemetriaCamion telemetria = new TelemetriaCamion();
                String id = UUID.randomUUID().toString().substring(0, 8);
                telemetria.setId(id);
                telemetria.setCamionId(camionId);
                telemetria.setTimestamp(timestamp);
                telemetria.setTemperatura(Math.round(temperatura * 100.0) / 100.0);
                telemetria.setHumedad(Math.round(humedad * 100.0) / 100.0);
                telemetria.setEstado(estado);

                // Serializar objeto a JSON String usando objectMapper
                String jsonMessage = objectMapper.writeValueAsString(telemetria);

                kafkaTemplate.send(topicName, camionId, jsonMessage);
            }

            return "¡Se han enviado 50 mensajes de simulación para los 3 camiones al tópico '" + topicName + "' con éxito!";
        } catch (Exception e) {
            return "Error al generar o enviar la simulación: " + e.getMessage();
        }
    }
}