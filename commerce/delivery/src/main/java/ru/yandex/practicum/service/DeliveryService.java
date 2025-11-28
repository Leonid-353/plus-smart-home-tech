package ru.yandex.practicum.service;

import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.order.OrderDto;

import java.math.BigDecimal;
import java.util.UUID;

public interface DeliveryService {

    DeliveryDto createDelivery(DeliveryDto deliveryDto);

    void emulationSuccessfulDelivery(UUID orderId);

    void emulationReceivingProductForDelivery(UUID orderId);

    void emulationFailedDelivery(UUID orderId);

    BigDecimal calculateDeliveryCost(OrderDto orderDto);
}
