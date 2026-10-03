package com.liquidity.events;

import java.math.BigDecimal;
import java.time.Instant;

public record TransactionEvent(String entity, String currency, 
    BigDecimal amount, Instant eventTimestamp, 
    String correspondent, String uetr
) {
}
