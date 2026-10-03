package com.liquidity.app.simulator;

import com.liquidity.events.Topics;
import com.liquidity.events.TransactionEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Feed simulator — the source of steady, realistic transaction traffic.
 *
 * <p>This component lives in the {@code app} module (not {@code events}) precisely because it
 * needs Spring and Kafka. {@code events} is the dependency-free contract; anything that talks to
 * infrastructure belongs here.
 *
 * <p>Note the single {@link #publish(TransactionEvent)} choke point: the scheduled job and, later,
 * the scenario-injection REST endpoints both go through it, so "normal" traffic and injected test
 * events travel the exact same code path.
 */
@Component
public class TransactionPublisher {

    // KafkaTemplate<KEY, VALUE>. KEY is the currency String (drives partitioning); VALUE is Object
    // so this one producer can also carry HeartbeatEvents later. The bean is defined explicitly in
    // KafkaConfig; JsonSerializer serializes whatever we pass and stamps the concrete type header.
    private final KafkaTemplate<String, Object> kafka;

    // Simulated dimensions. Small on purpose — enough variety to see partitioning at work.
    private static final List<String> ENTITIES = List.of("LON-TREASURY", "NY-TREASURY", "SG-TREASURY");
    private static final List<String> CURRENCIES = List.of("USD", "EUR", "GBP", "JPY");
    private static final List<String> CORRESPONDENTS = List.of("JPM", "DB", "BARC", "MUFG");

    public TransactionPublisher(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    /**
     * Every second, emit one normal transaction: event-time is "now" because it is happening now.
     * (A LATE event — event-time in the past, published now — is what the scenario endpoint will
     * inject later to exercise the aggregator's watermarks.)
     */
    @Scheduled(fixedRate = 1000)
    public void emitRandomTransaction() {
        String entity = pick(ENTITIES);
        String currency = pick(CURRENCIES);
        String correspondent = pick(CORRESPONDENTS);

        // Money as BigDecimal, built from a long → never from a double. Two decimal places.
        BigDecimal amount = BigDecimal.valueOf(ThreadLocalRandom.current().nextLong(1_000, 10_000_000))
                .movePointLeft(2); // e.g. 123456 -> 1234.56

        TransactionEvent event = new TransactionEvent(
                entity,
                currency,
                amount,
                Instant.now(),                 // event-time == now for normal traffic
                correspondent,
                UUID.randomUUID().toString()   // unique end-to-end reference
        );

        publish(event);
    }

    /**
     * The one place any transaction is sent. Keyed by CURRENCY — this key argument is the entire
     * reason "partition by currency" holds. Pass the wrong field here and the ordering guarantee
     * we reasoned about is silently lost.
     */
    public void publish(TransactionEvent event) {
        kafka.send(Topics.TRANSACTIONS, event.currency(), event);
    }

    private static String pick(List<String> options) {
        return options.get(ThreadLocalRandom.current().nextInt(options.size()));
    }
}
