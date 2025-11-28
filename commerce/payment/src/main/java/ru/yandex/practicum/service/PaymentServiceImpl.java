package ru.yandex.practicum.service;

import jakarta.ws.rs.NotFoundException;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.constant.Constants;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.payment.PaymentDto;
import ru.yandex.practicum.dto.payment.enums.PaymentState;
import ru.yandex.practicum.dto.store.ProductDto;
import ru.yandex.practicum.exception.NotEnoughInfoInOrderToCalculateException;
import ru.yandex.practicum.feign.OrderFeignClient;
import ru.yandex.practicum.feign.StoreFeignClient;
import ru.yandex.practicum.mapper.PaymentMapper;
import ru.yandex.practicum.model.Payment;
import ru.yandex.practicum.repository.PaymentRepository;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class PaymentServiceImpl implements PaymentService {
    final OrderFeignClient orderClient;
    final StoreFeignClient storeClient;
    final PaymentRepository paymentRepository;
    final PaymentMapper paymentMapper;

    @Override
    @Transactional
    public PaymentDto createPayment(OrderDto orderDto) {
        validateOrderForPayment(orderDto);

        if (paymentRepository.existsByOrderId(orderDto.getOrderId())) {
            throw new IllegalStateException(
                    String.format("Платеж для заказа %s уже существует", orderDto.getOrderId()));
        }

        Payment payment = buildPayment(orderDto);
        return paymentMapper.convertToDto(paymentRepository.save(payment));
    }

    @Override
    @Transactional
    public BigDecimal calculateTotalCost(OrderDto orderDto) {
        validateOrderForPayment(orderDto);

        Payment payment = findPaymentOrThrow(orderDto.getPaymentId());
        BigDecimal totalCost = computeTotalCost(orderDto);

        payment.setTotalPayment(totalCost);
        return totalCost;
    }

    @Override
    @Transactional
    public void emulationSuccessfulPayment(UUID paymentId) {
        Payment payment = findPaymentOrThrow(paymentId);

        orderClient.paymentOrder(payment.getOrderId());
        payment.setPaymentStatus(PaymentState.SUCCESS);
    }

    @Override
    public BigDecimal calculateProductCost(OrderDto orderDto) {
        Map<UUID, Long> products = orderDto.getProducts();

        Map<UUID, BigDecimal> productPrices = getProductPrices(products.keySet());

        return products.entrySet().stream()
                .map(entry -> computeProductCost(entry, productPrices))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    @Override
    @Transactional
    public void emulationFailedPayment(UUID paymentId) {
        Payment payment = findPaymentOrThrow(paymentId);

        orderClient.paymentOrderFailed(payment.getOrderId());
        payment.setPaymentStatus(PaymentState.FAILED);
    }

    // Auxiliary methods
    private Payment findPaymentOrThrow(UUID paymentId) {
        return paymentRepository.findById(paymentId)
                .orElseThrow(() -> new NotFoundException(
                        String.format(Constants.PAYMENT_NOT_FOUND_FORMAT, paymentId)));
    }

    private void validateOrderForPayment(OrderDto orderDto) {
        if (orderDto.getTotalPrice() == null) {
            throw new NotEnoughInfoInOrderToCalculateException("Не указана стоимость заказа");
        }
        if (orderDto.getDeliveryPrice() == null) {
            throw new NotEnoughInfoInOrderToCalculateException("Не указана стоимость доставки");
        }
        if (orderDto.getProductPrice() == null) {
            throw new NotEnoughInfoInOrderToCalculateException("Не указана стоимость товаров");
        }
    }

    private Payment buildPayment(OrderDto orderDto) {
        return Payment.builder()
                .orderId(orderDto.getOrderId())
                .totalPayment(computeTotalCost(orderDto))
                .deliveryTotal(orderDto.getDeliveryPrice())
                .feeTotal(calculateFee(orderDto))
                .paymentStatus(PaymentState.PENDING)
                .build();
    }

    private BigDecimal computeTotalCost(OrderDto orderDto) {
        BigDecimal productPrice = orderDto.getProductPrice();
        BigDecimal deliveryPrice = orderDto.getDeliveryPrice();
        BigDecimal feeAmount = calculateFee(orderDto);

        return productPrice.add(feeAmount).add(deliveryPrice);
    }

    private BigDecimal calculateFee(OrderDto orderDto) {
        return orderDto.getProductPrice().multiply(Constants.FEE_PERCENTAGE);
    }

    private Map<UUID, BigDecimal> getProductPrices(Set<UUID> productIds) {
        return productIds.stream()
                .collect(Collectors.toMap(
                        productId -> productId,
                        this::getProductPriceById
                ));
    }

    private BigDecimal getProductPriceById(UUID productId) {
        try {
            ProductDto productDto = storeClient.getProduct(productId);
            return productDto.getPrice();
        } catch (Exception e) {
            throw new NotFoundException("Не удалось получить цену для продукта: " + productId);
        }
    }

    private BigDecimal computeProductCost(Map.Entry<UUID, Long> productEntry,
                                          Map<UUID, BigDecimal> productPrices) {
        UUID productId = productEntry.getKey();
        Long quantity = productEntry.getValue();
        BigDecimal price = productPrices.get(productId);

        if (price == null) {
            throw new NotFoundException("Цена для продукта " + productId + " не найдена");
        }

        return price.multiply(BigDecimal.valueOf(quantity));
    }
}
