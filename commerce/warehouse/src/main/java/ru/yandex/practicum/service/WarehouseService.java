package ru.yandex.practicum.service;

import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.*;

import java.util.Map;
import java.util.UUID;

public interface WarehouseService {

    void createNewProductToWarehouse(NewProductInWarehouseRequest request);

    BookedProductsDto checkProductQuantity(ShoppingCartDto shoppingCartDto);

    void addProductToWarehouse(AddProductToWarehouseRequest request);

    AddressDto getAddressWarehouse();

    void shippedToDelivery(ShippedToDeliveryRequest request);

    void acceptReturnToWarehouse(Map<UUID, Long> returnedProducts);

    BookedProductsDto assemblyProductsForOrder(AssemblyProductsForOrderRequest request);
}
