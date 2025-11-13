package ru.yandex.practicum.feign;

import feign.FeignException;
import jakarta.validation.Valid;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;

import java.math.BigDecimal;
import java.util.UUID;

@FeignClient(name = "payment", path = "/api/v1/payment")
public interface PaymentFeignClient {

    @PostMapping
    PaymentDto createPayment(@Valid @RequestBody OrderDto orderDto) throws FeignException;

    @PostMapping("/totalCost")
    BigDecimal calculateTotalCost(@Valid @RequestBody OrderDto orderDto) throws FeignException;

    @PostMapping("/refund")
    void emulationSuccessfulPayment(@RequestBody UUID paymentId) throws FeignException;

    @PostMapping("/productCost")
    BigDecimal calculateProductCost(@Valid @RequestBody OrderDto orderDto) throws FeignException;

    @PostMapping("/failed")
    void emulationFailedPayment(@RequestBody UUID paymentId) throws FeignException;
}
