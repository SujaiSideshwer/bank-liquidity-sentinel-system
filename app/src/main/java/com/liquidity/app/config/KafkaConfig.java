package com.liquidity.app.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Explicit Kafka producer wiring.
 *
 * <p>Spring Boot auto-configures a {@code KafkaTemplate<?, ?>}, but a wildcard-typed bean does not
 * satisfy a strongly-typed injection point like {@code KafkaTemplate<String, TransactionEvent>}.
 * Defining the beans ourselves (a) removes that ambiguity and (b) puts the serialization choices
 * in plain sight instead of buried in autoconfiguration.
 *
 * <p>Value type is {@code Object} on purpose: this single producer publishes several event types
 * (transactions now, heartbeats next). {@link JacksonJsonSerializer} (Jackson 3, since Spring Boot 4
 * ships Jackson 3 — the Jackson 2 JsonSerializer is deprecated for removal) serializes any of them
 * and records the concrete class in a type header, so consumers can still recover the
 * exact type. Key stays {@code String} — it is the currency / feedId that drives partitioning.
 */
@Configuration
public class KafkaConfig {

    @Bean
    public ProducerFactory<String, Object> producerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
