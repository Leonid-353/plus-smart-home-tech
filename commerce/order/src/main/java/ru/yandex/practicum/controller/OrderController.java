package ru.yandex.practicum.controller;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.order.ProductReturnRequest;
import ru.yandex.practicum.feign.OrderFeignClient;
import ru.yandex.practicum.service.OrderService;

import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/order")
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class OrderController implements OrderFeignClient {
    final OrderService orderService;

    // Create
    @Override
    public OrderDto createOrder(CreateNewOrderRequest request) {
        log.info("Получен запрос на создание заказа на основе корзины с ID: {}",
                request.getShoppingCart().getShoppingCartId());
        return orderService.createOrder(request);
    }

    // Read
    @Override
    public List<OrderDto> getOrdersUser(String username) {
        log.info("Получен запрос на чтение заказов пользователя: {}", username);
        return orderService.getOrdersUser(username);
    }

    // Others
    @Override
    public OrderDto returnOrder(ProductReturnRequest request) {
        log.info("Получен запрос на возврат заказа с ID: {}", request.getOrderId());
        return orderService.returnOrder(request);
    }

    @Override
    public OrderDto paymentOrder(UUID orderId) {
        log.info("Получен запрос на оплату заказа с ID: {}", orderId);
        return orderService.paymentOrder(orderId);
    }

    @Override
    public OrderDto paymentOrderFailed(UUID orderId) {
        log.info("Оплата произошла с ошибкой. Заказ с ID: {}", orderId);
        return orderService.paymentOrderFailed(orderId);
    }

    @Override
    public OrderDto deliveryOrder(UUID orderId) {
        log.info("Получен запрос на доставку заказа с ID: {}", orderId);
        return orderService.deliveryOrder(orderId);
    }

    @Override
    public OrderDto deliveryOrderFailed(UUID orderId) {
        log.info("Доставка произошла с ошибкой. Заказ с ID: {}", orderId);
        return orderService.deliveryOrderFailed(orderId);
    }

    @Override
    public OrderDto completedOrder(UUID orderId) {
        log.info("Завершение заказа с ID: {}", orderId);
        return orderService.completedOrder(orderId);
    }

    @Override
    public OrderDto calculateTotalCostOrder(UUID orderId) {
        log.info("Получен запрос на расчёт стоимости заказа с ID: {}", orderId);
        return orderService.calculateTotalCostOrder(orderId);
    }

    @Override
    public OrderDto calculateDeliveryCostOrder(UUID orderId) {
        log.info("Получен запрос на расчёт стоимости доставки заказа с ID: {}", orderId);
        return orderService.calculateDeliveryCostOrder(orderId);
    }

    @Override
    public OrderDto assemblyOrder(UUID orderId) {
        log.info("Получен запрос на сборку заказа с ID: {}", orderId);
        return orderService.assemblyOrder(orderId);
    }

    @Override
    public OrderDto assemblyOrderFailed(UUID orderId) {
        log.info("Сборка произошла с ошибкой. Заказ с ID: {}", orderId);
        return orderService.assemblyOrderFailed(orderId);
    }
}
