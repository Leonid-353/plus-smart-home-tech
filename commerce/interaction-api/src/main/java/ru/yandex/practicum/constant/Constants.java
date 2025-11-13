package ru.yandex.practicum.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Constants {

    // Payment
    public static final BigDecimal FEE_PERCENTAGE = new BigDecimal("0.10");

    public static final String PAYMENT_NOT_FOUND_FORMAT = "Не найден платеж с ID: %s";

    // Delivery
    public static final BigDecimal BASE_RATE = new BigDecimal("5.0");
    public static final BigDecimal ADDRESS_1_RATE = new BigDecimal("1.0");
    public static final BigDecimal ADDRESS_2_RATE = new BigDecimal("2.0");
    public static final BigDecimal FRAGILE_RATE = new BigDecimal("0.2");
    public static final BigDecimal WEIGHT_RATE = new BigDecimal("0.3");
    public static final BigDecimal VOLUME_RATE = new BigDecimal("0.2");
    public static final BigDecimal DIFFERENT_STREET_RATE = new BigDecimal("0.2");

    public static final String DELIVERY_NOT_FOUND_BY_ORDER_ID_FORMAT = "Доставка не найдена по заказу с ID: %s";
    public static final String DELIVERY_NOT_FOUND_FORMAT = "Не найдена доставка с ID: %s";

    // Order
    public static final String ORDER_NOT_FOUND_FORMAT = "Не найден заказ с ID: %s";

    // Warehouse address
    public static final String WAREHOUSE_ADDRESS_1 = "ADDRESS_1";
    public static final String WAREHOUSE_ADDRESS_2 = "ADDRESS_2";
}
