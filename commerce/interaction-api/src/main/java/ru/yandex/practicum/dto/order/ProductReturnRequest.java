package ru.yandex.practicum.dto.order;

import jakarta.validation.constraints.NotEmpty;
import lombok.*;
import lombok.experimental.FieldDefaults;

import java.util.Map;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class ProductReturnRequest {
    UUID orderId;

    @NotEmpty
    Map<UUID, Long> products;
}
