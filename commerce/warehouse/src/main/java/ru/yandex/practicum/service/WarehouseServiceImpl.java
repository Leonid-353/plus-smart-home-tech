package ru.yandex.practicum.service;

import jakarta.ws.rs.NotFoundException;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.*;
import ru.yandex.practicum.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.exception.ProductInShoppingCartLowQuantityInWarehouse;
import ru.yandex.practicum.exception.SpecifiedProductAlreadyInWarehouseException;
import ru.yandex.practicum.mapper.WarehouseMapper;
import ru.yandex.practicum.model.Address;
import ru.yandex.practicum.model.Dimension;
import ru.yandex.practicum.model.OrderBooking;
import ru.yandex.practicum.model.WarehouseProduct;
import ru.yandex.practicum.repository.OrderBookingRepository;
import ru.yandex.practicum.repository.WarehouseRepository;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class WarehouseServiceImpl implements WarehouseService {
    final WarehouseRepository warehouseRepository;
    final OrderBookingRepository orderBookingRepository;
    final WarehouseMapper warehouseMapper;

    @Override
    @Transactional
    public void createNewProductToWarehouse(NewProductInWarehouseRequest request) {
        if (warehouseRepository.existsById(request.getProductId())) {
            throw new SpecifiedProductAlreadyInWarehouseException(
                    "Товар с таким описанием уже зарегистрирован на складе. ID: " + request.getProductId());
        }

        warehouseRepository.save(warehouseMapper.convertToEntity(request));
    }

    @Override
    @Transactional(readOnly = true)
    public BookedProductsDto checkProductQuantity(ShoppingCartDto shoppingCartDto) {
        Map<UUID, Long> cartProducts = shoppingCartDto.getProducts();

        Set<UUID> productIds = cartProducts.keySet();
        List<WarehouseProduct> warehouseProducts = warehouseRepository.findAllById(productIds);

        Map<UUID, WarehouseProduct> productMap = warehouseProducts.stream()
                .collect(Collectors.toMap(WarehouseProduct::getProductId, Function.identity()));

        Set<UUID> foundIds = productMap.keySet();
        Set<UUID> missingIds = productIds.stream()
                .filter(id -> !foundIds.contains(id))
                .collect(Collectors.toSet());

        if (!missingIds.isEmpty()) {
            throw new NoSpecifiedProductInWarehouseException(
                    "На складе отсутствуют товары с ID: " + missingIds
            );
        }

        DeliveryParams params = calculateDeliveryParams(cartProducts, productMap);

        return BookedProductsDto.builder()
                .deliveryWeight(params.totalDeliveryWeight())
                .deliveryVolume(params.totalDeliveryVolume())
                .fragile(params.hasFragileProduct())
                .build();
    }

    @Override
    @Transactional
    public void addProductToWarehouse(AddProductToWarehouseRequest request) {
        WarehouseProduct product = warehouseRepository.findById(request.getProductId())
                .orElseThrow(() -> new NoSpecifiedProductInWarehouseException(
                        "О товаре с ID: " + request.getProductId() + " нет информации на складе."));

        product.setQuantity(product.getQuantity() + request.getQuantity());

        warehouseRepository.save(product);
    }

    @Override
    public AddressDto getAddressWarehouse() {
        String address = Address.CURRENT_ADDRESS;

        return AddressDto.builder()
                .country(address)
                .city(address)
                .street(address)
                .house(address)
                .flat(address)
                .build();
    }

    @Override
    @Transactional
    public void shippedToDelivery(ShippedToDeliveryRequest request) {
        OrderBooking orderBooking = orderBookingRepository.findById(request.getOrderId())
                .orElseThrow(() -> new NotFoundException(
                        String.format("Бронирование с ID: %s не найдено", request.getOrderId())));

        orderBooking.setDeliveryId(request.getDeliveryId());
        orderBookingRepository.save(orderBooking);
    }

    @Override
    @Transactional
    public void acceptReturnToWarehouse(Map<UUID, Long> returnedProducts) {
        if (returnedProducts == null || returnedProducts.isEmpty()) {
            throw new IllegalArgumentException("Список возвращаемых товаров не может быть пустым");
        }

        returnedProducts.forEach((productId, quantity) -> {
            if (quantity == null || quantity <= 0) {
                throw new IllegalArgumentException(
                        String.format("Количество товара должно быть положительным числом: %s", productId));
            }
        });

        Set<UUID> productIds = returnedProducts.keySet();
        List<WarehouseProduct> warehouseProducts = warehouseRepository.findAllById(productIds);
        Map<UUID, WarehouseProduct> productMap = warehouseProducts.stream()
                .collect(Collectors.toMap(WarehouseProduct::getProductId, Function.identity()));

        Set<UUID> foundIds = productMap.keySet();
        Set<UUID> missingIds = productIds.stream()
                .filter(id -> !foundIds.contains(id))
                .collect(Collectors.toSet());

        if (!missingIds.isEmpty()) {
            throw new NotFoundException("Товары не найдены на складе: " + missingIds);
        }

        for (Map.Entry<UUID, Long> entry : returnedProducts.entrySet()) {
            WarehouseProduct product = productMap.get(entry.getKey());
            product.setQuantity(product.getQuantity() + entry.getValue());
        }

        warehouseRepository.saveAll(productMap.values());
    }

    @Override
    @Transactional
    public BookedProductsDto assemblyProductsForOrder(AssemblyProductsForOrderRequest request) {
        if (request.getProducts().isEmpty()) {
            throw new IllegalArgumentException("Список товаров не может быть пустым");
        }

        request.getProducts().forEach((productId, quantity) -> {
            if (quantity == null || quantity <= 0) {
                throw new IllegalArgumentException(
                        String.format("Количество товара должно быть положительным: %s", productId));
            }
        });

        if (orderBookingRepository.existsById(request.getOrderId())) {
            throw new IllegalArgumentException(
                    String.format("Бронирование для заказа %s уже существует", request.getOrderId()));
        }

        ShoppingCartDto shoppingCart = ShoppingCartDto.builder()
                .shoppingCartId(request.getOrderId())
                .products(request.getProducts())
                .build();

        BookedProductsDto bookedProductsDto = checkProductQuantity(shoppingCart);

        OrderBooking orderBooking = OrderBooking.builder()
                .orderId(request.getOrderId())
                .products(request.getProducts())
                .build();

        orderBookingRepository.save(orderBooking);

        return bookedProductsDto;
    }

    // Calculate delivery product params
    private DeliveryParams calculateDeliveryParams(Map<UUID, Long> cartProducts,
                                                   Map<UUID, WarehouseProduct> productMap) {

        double totalDeliveryWeight = 0.0;
        double totalDeliveryVolume = 0.0;
        boolean hasProductFragile = false;

        for (Map.Entry<UUID, Long> cartEntry : cartProducts.entrySet()) {
            UUID productId = cartEntry.getKey();
            long quantityRequested = cartEntry.getValue();
            WarehouseProduct warehouseProduct = productMap.get(productId);

            if (warehouseProduct.getQuantity() < quantityRequested) {
                throw new ProductInShoppingCartLowQuantityInWarehouse(
                        "На складе не хватает товара с ID: " + productId +
                                " (доступно: " + warehouseProduct.getQuantity() +
                                ", запрошено: " + quantityRequested + ")"
                );
            }

            if (Boolean.TRUE.equals(warehouseProduct.getFragile())) {
                hasProductFragile = true;
            }

            double totalProductWeight = warehouseProduct.getWeight() * quantityRequested;
            double totalProductVolume = calculateVolumeProduct(warehouseProduct.getDimension()) * quantityRequested;

            totalDeliveryWeight += totalProductWeight;
            totalDeliveryVolume += totalProductVolume;
        }

        return new DeliveryParams(totalDeliveryWeight, totalDeliveryVolume, hasProductFragile);
    }

    private double calculateVolumeProduct(Dimension dimension) {
        return dimension.getWidth() * dimension.getHeight() * dimension.getDepth();
    }

    private record DeliveryParams(
            double totalDeliveryWeight,
            double totalDeliveryVolume,
            boolean hasFragileProduct
    ) {
    }
}
