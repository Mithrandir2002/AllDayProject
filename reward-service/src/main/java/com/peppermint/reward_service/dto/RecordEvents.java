package com.peppermint.reward_service.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Common event envelope for messages published to Kafka.
 *
 * <p>Keep business data in {@code payload}; the remaining fields are metadata
 * used for tracing, compatibility, deduplication, and event routing.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "eventId", "eventType", "occurredAt", "source", "schemaVersion",
        "correlationId", "causationId", "payload"
})
public record RecordEvents<T>(
        String eventId,
        String eventType,
        Instant occurredAt,
        String source,
        int schemaVersion,
        String correlationId,
        String causationId,
        T payload
) {
    public RecordEvents {
        eventId = requireText(eventId, "eventId");
        eventType = requireText(eventType, "eventType");
        occurredAt = Objects.requireNonNull(occurredAt, "occurredAt is required");
        source = requireText(source, "source");
        if (schemaVersion < 1) {
            throw new IllegalArgumentException("schemaVersion must be at least 1");
        }
        Objects.requireNonNull(payload, "payload is required");
    }

    /** Creates a new event with a generated ID, current timestamp, and schema version 1. */
    public static <T> RecordEvents<T> of(String eventType, String source, T payload) {
        return new RecordEvents<>(UUID.randomUUID().toString(), eventType, Instant.now(),
                source, 1, null, null, payload);
    }

    /** Creates a new event and propagates trace context from its parent event. */
    public static <T> RecordEvents<T> causedBy(
            String eventType, String source, T payload, RecordEvents<?> parent) {
        Objects.requireNonNull(parent, "parent event is required");
        String correlationId = parent.correlationId() != null
                ? parent.correlationId()
                : parent.eventId();
        return new RecordEvents<>(UUID.randomUUID().toString(), eventType, Instant.now(),
                source, 1, correlationId, parent.eventId(), payload);
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }
}
