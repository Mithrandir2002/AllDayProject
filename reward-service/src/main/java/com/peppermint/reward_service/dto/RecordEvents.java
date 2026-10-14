package com.peppermint.reward_service.dto;

import java.time.Instant;
import java.util.Map;

public class RecordEvents {
    private String eventId;
    private String eventType;
    private Instant timestamp;
    private String source;
    private Map<String, Object> payload;
}
