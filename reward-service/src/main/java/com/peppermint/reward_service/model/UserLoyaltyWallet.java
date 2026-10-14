package com.peppermint.reward_service.model;

import com.peppermint.reward_service.enums.TierLevel;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "user_loyalty_wallet")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserLoyaltyWallet {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id", nullable = false, unique = true, updatable = false)
    private Long userId;

    @Column(name = "email", nullable = false, unique = true)
    private String email;

    @Column(name = "first_name", nullable = false)
    private String firstName;

    @Column(name = "last_name", nullable = false)
    private String lastName;

    @Builder.Default
    @Column(name = "total_points", nullable = false)
    private Integer totalPoints = 0;

    @Builder.Default
    @Column(name = "life_time_poins", nullable = false)
    private Integer lifeTimePoints = 0;

    @Enumerated(EnumType.STRING)
    @Column(name = "tier_level", nullable = false)
    private TierLevel tierLevel;

    @Version
    private Long version;

    @CreationTimestamp
    private LocalDateTime createdDate;

    @UpdateTimestamp
    private LocalDateTime updatedDate;
}
