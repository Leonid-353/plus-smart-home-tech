package ru.yandex.practicum.controller;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.feign.DeliveryFeignClient;
import ru.yandex.practicum.service.DeliveryService;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/delivery")
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class DeliveryController implements DeliveryFeignClient {
    final DeliveryService deliveryService;

    @Override
    public DeliveryDto createDelivery(DeliveryDto deliveryDto) {
        log.info("Получен запрос на создание новой доставки. Заказ с ID: {}", deliveryDto.getOrderId());
        return deliveryService.createDelivery(deliveryDto);
    }

    @Override
    public void emulationSuccessfulDelivery(UUID orderId) {
        log.info("Получен запрос для эмуляции успешной доставки товара. Заказ с ID: {}", orderId);
        deliveryService.emulationSuccessfulDelivery(orderId);
    }

    @Override
    public void emulationReceivingProductForDelivery(UUID orderId) {
        log.info("Получен запрос для эмуляции получения товара в доставку. Заказ с ID: {}", orderId);
        deliveryService.emulationReceivingProductForDelivery(orderId);
    }

    @Override
    public void emulationFailedDelivery(UUID orderId) {
        log.info("Получен запрос для эмуляции неудачного вручения товара. Заказ с ID: {}", orderId);
        deliveryService.emulationFailedDelivery(orderId);
    }

    @Override
    public BigDecimal calculateDeliveryCost(OrderDto orderDto) {
        log.info("Получен запрос на расчёт полной стоимости доставки заказа с ID: {}", orderDto.getOrderId());
        return deliveryService.calculateDeliveryCost(orderDto);
    }
}
