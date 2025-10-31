package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;
import lombok.experimental.FieldDefaults;

import java.util.UUID;

@Entity
@Table(name = "warehouse_products")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class WarehouseProduct {

    @Id
    @Column(name = "product_id", nullable = false, unique = true)
    UUID productId;

    @Column(nullable = false)
    Boolean fragile;

    @Embedded
    Dimension dimension;

    @Column(nullable = false)
    Double weight;

    @Column
    @Builder.Default
    Long quantity = 0L;
}
