package ru.yandex.practicum.service;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.constant.Constants;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.dto.delivery.enums.DeliveryState;
import ru.yandex.practicum.dto.order.OrderDto;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.ShippedToDeliveryRequest;
import ru.yandex.practicum.exception.NoDeliveryFoundException;
import ru.yandex.practicum.feign.OrderFeignClient;
import ru.yandex.practicum.feign.WarehouseFeignClient;
import ru.yandex.practicum.mapper.DeliveryMapper;
import ru.yandex.practicum.model.Delivery;
import ru.yandex.practicum.repository.DeliveryRepository;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class DeliveryServiceImpl implements DeliveryService {
    final WarehouseFeignClient warehouseClient;
    final OrderFeignClient orderClient;
    final DeliveryRepository deliveryRepository;
    final DeliveryMapper deliveryMapper;

    @Override
    @Transactional
    public DeliveryDto createDelivery(DeliveryDto deliveryDto) {
        if (deliveryRepository.existsByOrderId(deliveryDto.getOrderId())) {
            throw new IllegalStateException(
                    String.format("Доставка для заказа %s уже существует", deliveryDto.getOrderId()));
        }

        Delivery delivery = deliveryMapper.convertToEntity(deliveryDto);
        delivery.setDeliveryState(DeliveryState.CREATED);

        return deliveryMapper.convertToDto(deliveryRepository.save(delivery));
    }

    @Override
    @Transactional
    public void emulationSuccessfulDelivery(UUID orderId) {
        Delivery delivery = findDeliveryByOrderIdOrThrow(orderId);

        orderClient.completedOrder(delivery.getOrderId());
        delivery.setDeliveryState(DeliveryState.DELIVERED);
    }

    @Override
    @Transactional
    public void emulationReceivingProductForDelivery(UUID orderId) {
        Delivery delivery = findDeliveryByOrderIdOrThrow(orderId);

        orderClient.assemblyOrder(delivery.getOrderId());

        ShippedToDeliveryRequest request = buildShippedToDeliveryRequest(delivery);
        warehouseClient.shippedToDelivery(request);

        delivery.setDeliveryState(DeliveryState.IN_PROGRESS);
    }

    @Override
    @Transactional
    public void emulationFailedDelivery(UUID orderId) {
        Delivery delivery = findDeliveryByOrderIdOrThrow(orderId);

        orderClient.deliveryOrderFailed(delivery.getOrderId());
        delivery.setDeliveryState(DeliveryState.FAILED);
    }

    @Override
    @Transactional(readOnly = true)
    public BigDecimal calculateDeliveryCost(OrderDto orderDto) {
        Delivery delivery = findDeliveryOrThrow(orderDto.getDeliveryId());
        AddressDto warehouseAddress = warehouseClient.getAddressWarehouse();

        BigDecimal cost = Constants.BASE_RATE;

        cost = applyWarehouseAddressCost(cost, warehouseAddress.getStreet());
        cost = applyFragileCost(cost, orderDto.getFragile());
        cost = applyWeightCost(cost, orderDto.getDeliveryWeight());
        cost = applyVolumeCost(cost, orderDto.getDeliveryVolume());
        cost = applyDeliveryAddressCost(cost, delivery.getToAddress().getStreet(), warehouseAddress.getStreet());

        return cost;
    }

    // Auxiliary methods
    private Delivery findDeliveryByOrderIdOrThrow(UUID orderId) {
        return deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new NoDeliveryFoundException(
                        String.format(Constants.DELIVERY_NOT_FOUND_BY_ORDER_ID_FORMAT, orderId)));
    }

    private Delivery findDeliveryOrThrow(UUID deliveryId) {
        return deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new NoDeliveryFoundException(
                        String.format(Constants.DELIVERY_NOT_FOUND_FORMAT, deliveryId)));
    }

    private ShippedToDeliveryRequest buildShippedToDeliveryRequest(Delivery delivery) {
        return ShippedToDeliveryRequest.builder()
                .orderId(delivery.getOrderId())
                .deliveryId(delivery.getDeliveryId())
                .build();
    }

    // Apply coefficient
    private BigDecimal applyWarehouseAddressCost(BigDecimal currentCost, String warehouseStreet) {
        BigDecimal rate;

        if (Constants.WAREHOUSE_ADDRESS_1.equals(warehouseStreet)) {
            rate = Constants.ADDRESS_1_RATE;
        } else if (Constants.WAREHOUSE_ADDRESS_2.equals(warehouseStreet)) {
            rate = Constants.ADDRESS_2_RATE;
        } else {
            rate = Constants.ADDRESS_1_RATE;
        }

        BigDecimal multipliedCost = currentCost.multiply(rate);
        return currentCost.add(multipliedCost);
    }

    private BigDecimal applyFragileCost(BigDecimal currentCost, Boolean isFragile) {
        if (Boolean.TRUE.equals(isFragile)) {
            BigDecimal fragileCost = currentCost.multiply(Constants.FRAGILE_RATE);
            return currentCost.add(fragileCost);
        }
        return currentCost;
    }

    private BigDecimal applyWeightCost(BigDecimal currentCost, Double deliveryWeight) {
        if (deliveryWeight != null) {
            BigDecimal weightCost = BigDecimal.valueOf(deliveryWeight).multiply(Constants.WEIGHT_RATE);
            return currentCost.add(weightCost);
        }
        return currentCost;
    }

    private BigDecimal applyVolumeCost(BigDecimal currentCost, Double deliveryVolume) {
        if (deliveryVolume != null) {
            BigDecimal volumeCost = BigDecimal.valueOf(deliveryVolume).multiply(Constants.VOLUME_RATE);
            return currentCost.add(volumeCost);
        }
        return currentCost;
    }

    private BigDecimal applyDeliveryAddressCost(BigDecimal currentCost,
                                                String deliveryStreet,
                                                String warehouseStreet) {
        if (deliveryStreet != null && !deliveryStreet.equals(warehouseStreet)) {
            BigDecimal addressCost = currentCost.multiply(Constants.DIFFERENT_STREET_RATE);
            return currentCost.add(addressCost);
        }
        return currentCost;
    }
}
