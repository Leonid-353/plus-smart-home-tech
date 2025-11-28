package ru.yandex.practicum.feign;

import feign.FeignException;
import jakarta.validation.Valid;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.order.ProductReturnRequest;

import java.util.List;
import java.util.UUID;

@FeignClient(name = "order", path = "/api/v1/order")
public interface OrderFeignClient {

    // Create
    @PutMapping
    OrderDto createOrder(@Valid @RequestBody CreateNewOrderRequest request) throws FeignException;

    // Read
    @GetMapping
    List<OrderDto> getOrdersUser(@RequestParam String username) throws FeignException;

    // Others
    @PostMapping("/return")
    OrderDto returnOrder(@Valid @RequestBody ProductReturnRequest request) throws FeignException;

    @PostMapping("/payment")
    OrderDto paymentOrder(@RequestBody UUID orderId) throws FeignException;

    @PostMapping("/payment/failed")
    OrderDto paymentOrderFailed(@RequestBody UUID orderId) throws FeignException;

    @PostMapping("/delivery")
    OrderDto deliveryOrder(@RequestBody UUID orderId) throws FeignException;

    @PostMapping("/delivery/failed")
    OrderDto deliveryOrderFailed(@RequestBody UUID orderId) throws FeignException;

    @PostMapping("/completed")
    OrderDto completedOrder(@RequestBody UUID orderId) throws FeignException;

    @PostMapping("/calculate/total")
    OrderDto calculateTotalCostOrder(@RequestBody UUID orderId) throws FeignException;

    @PostMapping("/calculate/delivery")
    OrderDto calculateDeliveryCostOrder(@RequestBody UUID orderId) throws FeignException;

    @PostMapping("/assembly")
    OrderDto assemblyOrder(@RequestBody UUID orderId) throws FeignException;

    @PostMapping("/assembly/failed")
    OrderDto assemblyOrderFailed(@RequestBody UUID orderId) throws FeignException;
}
