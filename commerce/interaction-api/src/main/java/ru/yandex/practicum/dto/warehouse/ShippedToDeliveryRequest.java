package ru.yandex.practicum.dto.warehouse;

import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.FieldDefaults;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class ShippedToDeliveryRequest {

    @NotNull(message = "ID заказа обязателен")
    UUID orderId;

    @NotNull(message = "ID доставки обязателен")
    UUID deliveryId;
}
