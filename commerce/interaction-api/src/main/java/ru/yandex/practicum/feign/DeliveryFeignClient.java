package ru.yandex.practicum.feign;

import feign.FeignException;
import jakarta.validation.Valid;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.order.OrderDto;

import java.math.BigDecimal;
import java.util.UUID;

@FeignClient(name = "delivery", path = "/api/v1/delivery")
public interface DeliveryFeignClient {

    @PutMapping
    DeliveryDto createDelivery(@Valid @RequestBody DeliveryDto deliveryDto) throws FeignException;

    @PostMapping("/successful")
    void emulationSuccessfulDelivery(@RequestBody UUID orderId) throws FeignException;

    @PostMapping("/picked")
    void emulationReceivingProductForDelivery(@RequestBody UUID orderId) throws FeignException;

    @PostMapping("/failed")
    void emulationFailedDelivery(@RequestBody UUID orderId) throws FeignException;

    @PostMapping("/cost")
    BigDecimal calculateDeliveryCost(@Valid @RequestBody OrderDto orderDto) throws FeignException;
}
