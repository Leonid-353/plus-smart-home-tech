package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.hibernate.annotations.UuidGenerator;
import ru.yandex.practicum.dto.payment.enums.PaymentState;

import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "payments")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class Payment {

    @Id
    @UuidGenerator
    @Column(name = "payment_id")
    UUID paymentId;

    @Column(name = "order_id")
    UUID orderId;

    @Column(name = "total_payment")
    BigDecimal totalPayment;    // Общая стоимость.

    @Column(name = "delivery_total")
    BigDecimal deliveryTotal;   // Стоимость доставки.

    @Column(name = "fee_total")
    BigDecimal feeTotal;        // Стоимость налога.

    @Enumerated(EnumType.STRING)
    @Column(name = "payment_state")
    PaymentState paymentStatus;
}
