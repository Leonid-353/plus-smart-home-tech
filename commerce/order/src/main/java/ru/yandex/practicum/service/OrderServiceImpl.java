package ru.yandex.practicum.service;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.constant.Constants;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.delivery.enums.DeliveryState;
import ru.yandex.practicum.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.order.ProductReturnRequest;
import ru.yandex.practicum.dto.order.enums.OrderState;
import ru.yandex.practicum.dto.payment.PaymentDto;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.AssemblyProductsForOrderRequest;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.exception.NoOrderFoundException;
import ru.yandex.practicum.exception.NotAuthorizedUserException;
import ru.yandex.practicum.feign.CartFeignClient;
import ru.yandex.practicum.feign.DeliveryFeignClient;
import ru.yandex.practicum.feign.PaymentFeignClient;
import ru.yandex.practicum.feign.WarehouseFeignClient;
import ru.yandex.practicum.mapper.OrderMapper;
import ru.yandex.practicum.model.Order;
import ru.yandex.practicum.repository.OrderRepository;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class OrderServiceImpl implements OrderService {
    final CartFeignClient cartClient;
    final DeliveryFeignClient deliveryClient;
    final PaymentFeignClient paymentClient;
    final WarehouseFeignClient warehouseClient;
    final OrderRepository orderRepository;
    final OrderMapper orderMapper;

    // Create
    @Override
    @Transactional
    public OrderDto createOrder(CreateNewOrderRequest request) {
        // Создаем заказ с минимальными данными
        Order order = buildInitialOrder(request);
        orderRepository.save(order);

        // Резервируем продукты на складе и получаем характеристики
        BookedProductsDto bookedProducts =
                reserveProducts(order.getOrderId(), request.getShoppingCart().getProducts());

        // Создаем доставку
        DeliveryDto delivery = createDelivery(order.getOrderId(), request.getDeliveryAddress());

        // Обновляем заказ данными о продуктах и доставке
        updateOrderWithProductsAndDelivery(order, bookedProducts, delivery);

        // Рассчитываем стоимости
        calculateAndSetCosts(order);

        // Создаем платеж
        PaymentDto payment = createPayment(order);
        order.setPaymentId(payment.getPaymentId());

        return orderMapper.convertToDto(order);
    }


    // Read
    @Override
    @Transactional(readOnly = true)
    public List<OrderDto> getOrdersUser(String username) {
        validateUsername(username);

        ShoppingCartDto shoppingCartDto = cartClient.getCartForUser(username);
        if (shoppingCartDto == null || shoppingCartDto.getShoppingCartId() == null) {
            return List.of();
        }

        return orderRepository.findByShoppingCartId(shoppingCartDto.getShoppingCartId()).stream()
                .map(orderMapper::convertToDto)
                .toList();
    }

    // Others
    @Override
    @Transactional
    public OrderDto returnOrder(ProductReturnRequest request) {
        Order order = findOrderOrThrow(request.getOrderId());

        warehouseClient.acceptReturnToWarehouse(request.getProducts());
        order.setState(OrderState.PRODUCT_RETURNED);
        return orderMapper.convertToDto(order);
    }

    @Override
    @Transactional
    public OrderDto paymentOrder(UUID orderId) {
        Order order = findOrderOrThrow(orderId);

        order.setState(OrderState.PAID);
        return orderMapper.convertToDto(order);
    }

    @Override
    @Transactional
    public OrderDto paymentOrderFailed(UUID orderId) {
        Order order = findOrderOrThrow(orderId);

        order.setState(OrderState.PAYMENT_FAILED);
        return orderMapper.convertToDto(order);
    }

    @Override
    @Transactional
    public OrderDto deliveryOrder(UUID orderId) {
        Order order = findOrderOrThrow(orderId);

        deliveryClient.emulationSuccessfulDelivery(orderId);
        order.setState(OrderState.DELIVERED);
        return orderMapper.convertToDto(order);
    }

    @Override
    @Transactional
    public OrderDto deliveryOrderFailed(UUID orderId) {
        Order order = findOrderOrThrow(orderId);

        deliveryClient.emulationFailedDelivery(orderId);
        order.setState(OrderState.DELIVERY_FAILED);
        return orderMapper.convertToDto(order);
    }

    @Override
    @Transactional
    public OrderDto completedOrder(UUID orderId) {
        Order order = findOrderOrThrow(orderId);

        order.setState(OrderState.COMPLETED);
        return orderMapper.convertToDto(order);
    }

    @Override
    @Transactional
    public OrderDto calculateTotalCostOrder(UUID orderId) {
        Order order = findOrderOrThrow(orderId);
        OrderDto orderDto = orderMapper.convertToDto(order);

        order.setTotalPrice(paymentClient.calculateTotalCost(orderDto));
        return orderMapper.convertToDto(order);
    }

    @Override
    @Transactional
    public OrderDto calculateDeliveryCostOrder(UUID orderId) {
        Order order = findOrderOrThrow(orderId);
        OrderDto orderDto = orderMapper.convertToDto(order);

        order.setDeliveryPrice(deliveryClient.calculateDeliveryCost(orderDto));
        return orderMapper.convertToDto(order);
    }

    @Override
    @Transactional
    public OrderDto assemblyOrder(UUID orderId) {
        Order order = findOrderOrThrow(orderId);

        order.setState(OrderState.ASSEMBLED);
        return orderMapper.convertToDto(order);
    }

    @Override
    @Transactional
    public OrderDto assemblyOrderFailed(UUID orderId) {
        Order order = findOrderOrThrow(orderId);

        order.setState(OrderState.ASSEMBLY_FAILED);
        return orderMapper.convertToDto(order);
    }


    // Auxiliary methods
    private Order findOrderOrThrow(UUID orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException(
                        String.format(Constants.ORDER_NOT_FOUND_FORMAT, orderId)));
    }

    private Order buildInitialOrder(CreateNewOrderRequest request) {
        return Order.builder()
                .shoppingCartId(request.getShoppingCart().getShoppingCartId())
                .products(new HashMap<>(request.getShoppingCart().getProducts()))
                .state(OrderState.NEW)
                .build();
    }

    private BookedProductsDto reserveProducts(UUID orderId, Map<UUID, Long> products) {
        AssemblyProductsForOrderRequest assemblyRequest = AssemblyProductsForOrderRequest.builder()
                .orderId(orderId)
                .products(products)
                .build();

        return warehouseClient.assemblyProductsForOrder(assemblyRequest);
    }

    private DeliveryDto createDelivery(UUID orderId, AddressDto deliveryAddress) {
        DeliveryDto deliveryDto = DeliveryDto.builder()
                .orderId(orderId)
                .fromAddress(warehouseClient.getAddressWarehouse())
                .toAddress(deliveryAddress)
                .deliveryState(DeliveryState.CREATED)
                .build();

        return deliveryClient.createDelivery(deliveryDto);
    }

    private void updateOrderWithProductsAndDelivery(Order order, BookedProductsDto bookedProducts,
                                                    DeliveryDto delivery) {
        order.setDeliveryWeight(bookedProducts.getDeliveryWeight());
        order.setDeliveryVolume(bookedProducts.getDeliveryVolume());
        order.setFragile(bookedProducts.getFragile());
        order.setDeliveryId(delivery.getDeliveryId());
    }

    private void calculateAndSetCosts(Order order) {
        OrderDto orderDto = orderMapper.convertToDto(order);

        BigDecimal productPrice = paymentClient.calculateProductCost(orderDto);
        order.setProductPrice(productPrice);

        BigDecimal deliveryPrice = deliveryClient.calculateDeliveryCost(orderDto);
        order.setDeliveryPrice(deliveryPrice);

        orderDto.setProductPrice(productPrice);
        orderDto.setDeliveryPrice(deliveryPrice);
        orderDto.setTotalPrice(productPrice.add(deliveryPrice));

        order.setTotalPrice(orderDto.getTotalPrice());
    }

    private PaymentDto createPayment(Order order) {
        return paymentClient.createPayment(orderMapper.convertToDto(order));
    }

    // Validation methods
    private void validateUsername(String username) {
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Имя пользователя не может быть пустым");
        }
    }
}
