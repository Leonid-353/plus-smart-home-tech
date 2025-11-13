package ru.yandex.practicum.dto.delivery;

import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.FieldDefaults;
import ru.yandex.practicum.dto.delivery.enums.DeliveryState;
import ru.yandex.practicum.dto.warehouse.AddressDto;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class DeliveryDto {

    UUID deliveryId;

    @NotNull
    AddressDto fromAddress;

    @NotNull
    AddressDto toAddress;

    @NotNull
    UUID orderId;

    @NotNull
    DeliveryState deliveryState;

    @NotNull
    Double weight;

    @NotNull
    Double volume;

    @NotNull
    Boolean fragile;
}
