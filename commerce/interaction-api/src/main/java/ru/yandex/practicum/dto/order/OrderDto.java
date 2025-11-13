package ru.yandex.practicum.dto.order;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.FieldDefaults;
import ru.yandex.practicum.dto.order.enums.OrderState;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class OrderDto {

    @NotNull
    UUID orderId;

    UUID shoppingCartId;

    @NotEmpty
    Map<UUID, Long> products;

    UUID paymentId;

    UUID deliveryId;

    OrderState state;

    Double deliveryWeight;      // Общий вес доставки.

    Double deliveryVolume;      // Общий объём доставки.

    Boolean fragile;

    BigDecimal totalPrice;      // Общая стоимость.

    BigDecimal deliveryPrice;   // Стоимость доставки.

    BigDecimal productPrice;    // Стоимость товаров в заказе.
}
