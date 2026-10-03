package com.liquidity.events;

import java.time.Instant;

public record HeartbeatEvent(String feedId, 
    Instant timestamp
) {
}
