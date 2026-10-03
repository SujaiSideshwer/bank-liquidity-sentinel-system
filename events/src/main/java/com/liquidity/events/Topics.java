package com.liquidity.events;

/**
 * Kafka topic names — part of the shared contract, not service config.
 *
 * <p>A producer and a consumer that disagree on a topic name don't error; the messages simply
 * go nowhere. Centralising the names here (in the dependency-free contract module every service
 * already imports) means there is exactly one source of truth and no chance of drift when the
 * modules later split into separate apps.
 */
public final class Topics {

    /** Currency-keyed stream of {@link TransactionEvent}s. Partitioned by currency. */
    public static final String TRANSACTIONS = "transactions";

    /** Per-feed liveness stream of {@link HeartbeatEvent}s. Keyed by feedId. */
    public static final String HEARTBEATS = "heartbeats";

    private Topics() {
        // constants holder — never instantiated
    }
}
