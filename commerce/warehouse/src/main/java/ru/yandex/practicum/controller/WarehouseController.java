package ru.yandex.practicum.controller;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.dto.warehouse.AddressDto;
import ru.yandex.practicum.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.dto.warehouse.NewProductInWarehouseRequest;
import ru.yandex.practicum.feign.WarehouseFeignClient;
import ru.yandex.practicum.service.WarehouseService;

@Slf4j
@RestController
@RequestMapping("/api/v1/warehouse")
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class WarehouseController implements WarehouseFeignClient {
    final WarehouseService warehouseService;

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
}
