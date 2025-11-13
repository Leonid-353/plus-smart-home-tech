package ru.yandex.practicum.controller;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.*;
import ru.yandex.practicum.feign.WarehouseFeignClient;
import ru.yandex.practicum.service.WarehouseService;

import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/warehouse")
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class WarehouseController implements WarehouseFeignClient {
    final WarehouseService warehouseService;

    // 21
    @Override
    public void createNewProductToWarehouse(NewProductInWarehouseRequest request) {
        log.info("Получен запрос на добавление нового товара на склад");
        warehouseService.createNewProductToWarehouse(request);
    }

    @Override
    public BookedProductsDto checkProductQuantity(ShoppingCartDto shoppingCartDto) {
        log.info("Получен запрос проверки достаточности товаров на складе для корзины {}", shoppingCartDto);
        return warehouseService.checkProductQuantity(shoppingCartDto);
    }

    @Override
    public void addProductToWarehouse(AddProductToWarehouseRequest request) {
        log.info("Получен запрос на принятие товара на склад");
        warehouseService.addProductToWarehouse(request);
    }

    @Override
    public AddressDto getAddressWarehouse() {
        return warehouseService.getAddressWarehouse();
    }

    // 22
    @Override
    public void shippedToDelivery(ShippedToDeliveryRequest request) {
        log.info("Получен запрос на передачу товаров в доставку");
        warehouseService.shippedToDelivery(request);
    }

    @Override
    public void acceptReturnToWarehouse(Map<UUID, Long> returnedProducts) {
        log.info("Получен запрос на возврат товаров на склад");
        warehouseService.acceptReturnToWarehouse(returnedProducts);
    }

    @Override
    public BookedProductsDto assemblyProductsForOrder(AssemblyProductsForOrderRequest request) {
        log.info("Получен запрос на сборку товаров к заказу: {} для подготовки к отправке", request.getOrderId());
        return warehouseService.assemblyProductsForOrder(request);
    }
}
