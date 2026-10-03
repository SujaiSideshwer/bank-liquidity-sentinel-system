package com.liquidity.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the liquidity-sentinel modular monolith.
 *
 * <p>Every service module (simulator, aggregation, ledger, position, sweep, ...) is a package
 * under {@code com.liquidity.app}. They are wired together by Spring here, but communicate with
 * each other only through Kafka topics, the Postgres ledger, and Redis — the same seams that
 * would exist if they were separate deployables. Keeping to those seams is what makes the
 * eventual split into microservices a repackaging job rather than a rewrite.
 */
@SpringBootApplication
@EnableScheduling // required for @Scheduled publishers (feed simulator) to fire
public class LiquiditySentinelApplication {

    public static void main(String[] args) {
        SpringApplication.run(LiquiditySentinelApplication.class, args);
    }
}
