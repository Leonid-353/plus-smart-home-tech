package ru.yandex.practicum.service;

import ru.yandex.practicum.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.order.ProductReturnRequest;

import java.util.List;
import java.util.UUID;

public interface OrderService {

    // Create
    OrderDto createOrder(CreateNewOrderRequest request);

    // Read
    List<OrderDto> getOrdersUser(String username);

    // Others
    OrderDto returnOrder(ProductReturnRequest request);

    OrderDto paymentOrder(UUID orderId);

    OrderDto paymentOrderFailed(UUID orderId);

    OrderDto deliveryOrder(UUID orderId);

    OrderDto deliveryOrderFailed(UUID orderId);

    OrderDto completedOrder(UUID orderId);

    OrderDto calculateTotalCostOrder(UUID orderId);

    OrderDto calculateDeliveryCostOrder(UUID orderId);

    OrderDto assemblyOrder(UUID orderId);

    OrderDto assemblyOrderFailed(UUID orderId);
}
