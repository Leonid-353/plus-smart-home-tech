package ru.yandex.practicum.controller;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;
import ru.yandex.practicum.feign.PaymentFeignClient;
import ru.yandex.practicum.service.PaymentService;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/payment")
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class PaymentController implements PaymentFeignClient {
    final PaymentService paymentService;

    @Override
    public PaymentDto createPayment(OrderDto orderDto) {
        log.info("Получен запрос на формирование оплаты заказа с ID: {} (переход в платёжный шлюз)",
                orderDto.getOrderId());
        return paymentService.createPayment(orderDto);
    }

    @Override
    public BigDecimal calculateTotalCost(OrderDto orderDto) {
        log.info("Получен запрос на расчёт полной стоимости заказа с ID: {}", orderDto.getOrderId());
        return paymentService.calculateTotalCost(orderDto);
    }

    @Override
    public void emulationSuccessfulPayment(UUID paymentId) {
        log.info("Получен запрос для эмуляции успешной оплаты платежного шлюза");
        paymentService.emulationSuccessfulPayment(paymentId);
    }

    @Override
    public BigDecimal calculateProductCost(OrderDto orderDto) {
        log.info("Получен запрос на расчёт стоимости товаров в заказе с ID: {}", orderDto.getOrderId());
        return paymentService.calculateProductCost(orderDto);
    }

    @Override
    public void emulationFailedPayment(UUID paymentId) {
        log.info("Получен запрос для эмуляции отказа в оплате платежного шлюза");
        paymentService.emulationFailedPayment(paymentId);
    }
}
