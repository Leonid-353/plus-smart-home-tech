package ru.yandex.practicum.dto.payment;

import lombok.*;
import lombok.experimental.FieldDefaults;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class PaymentDto {
    UUID paymentId;
    BigDecimal totalPayment;    // Общая стоимость.
    BigDecimal deliveryTotal;   // Стоимость доставки.
    BigDecimal feeTotal;        // Стоимость налога.
}
