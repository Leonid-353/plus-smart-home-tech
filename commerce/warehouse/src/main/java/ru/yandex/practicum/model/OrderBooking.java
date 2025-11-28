package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;
import lombok.experimental.FieldDefaults;

import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "order_booking")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class OrderBooking {
    @Id
    @Column(name = "order_id")
    UUID orderId;

    @Column(name = "delivery_id")
    UUID deliveryId;

    @CollectionTable(name = "booking_products", joinColumns = @JoinColumn(name = "order_booking_id"))
    @ElementCollection
    @MapKeyColumn(name = "product_id")
    @Column(name = "quantity")
    Map<UUID, Long> products;
}
