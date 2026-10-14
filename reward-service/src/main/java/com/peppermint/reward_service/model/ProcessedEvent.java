package com.peppermint.reward_service.model;

import com.peppermint.reward_service.enums.EventType;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "processed_event")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ProcessedEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "transaction_code", nullable = false, updatable = false)
    private String transactionCode;

    @ManyToOne(fetch = FetchType.LAZY,  optional = false)
    @JoinColumn(name = "user_loyalty_id")
    private UserLoyaltyWallet userLoyaltyWallet;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, updatable = false)
    private EventType eventType;

    @CreationTimestamp
    private LocalDateTime createdAt;
}
