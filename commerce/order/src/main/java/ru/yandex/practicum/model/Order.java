package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.hibernate.annotations.UuidGenerator;
import ru.yandex.practicum.dto.order.enums.OrderState;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class Order {

    @Id
    @UuidGenerator
    @Column(name = "order_id")
    UUID orderId;

    @Enumerated(EnumType.STRING)
    OrderState state;

    @ElementCollection
    @CollectionTable(name = "order_products", joinColumns = @JoinColumn(name = "order_id"))
    @MapKeyColumn(name = "product_id")
    @Column(name = "quantity")
    @Builder.Default
    Map<UUID, Long> products = new HashMap<>();

    @Column(name = "shopping_cart_id")
    UUID shoppingCartId;

    @Column(name = "delivery_id")
    UUID deliveryId;

    @Column(name = "payment_id")
    UUID paymentId;

    @Column(name = "delivery_volume")
    Double deliveryVolume;      // Общий объём доставки.

    @Column(name = "delivery_weight")
    Double deliveryWeight;      // Общий вес доставки.

    @Column(name = "fragile")
    Boolean fragile;

    @Column(name = "total_price")
    BigDecimal totalPrice;      // Общая стоимость.

    @Column(name = "product_price")
    BigDecimal productPrice;    // Стоимость товаров в заказе.

    @Column(name = "delivery_price")
    BigDecimal deliveryPrice;   // Стоимость доставки.
}
