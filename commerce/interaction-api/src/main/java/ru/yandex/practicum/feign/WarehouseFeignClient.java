package ru.yandex.practicum.feign;

import feign.FeignException;
import jakarta.validation.Valid;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.dto.warehouse.*;

import java.util.Map;
import java.util.UUID;

@FeignClient(name = "warehouse", path = "/api/v1/warehouse")
public interface WarehouseFeignClient {

    // 21
    @PutMapping
    void createNewProductToWarehouse(@Valid @RequestBody NewProductInWarehouseRequest request) throws FeignException;

    @PostMapping("/check")
    BookedProductsDto checkProductQuantity(@Valid @RequestBody ShoppingCartDto shoppingCartDto) throws FeignException;

    @PostMapping("/add")
    void addProductToWarehouse(@Valid @RequestBody AddProductToWarehouseRequest request) throws FeignException;

    @GetMapping("/address")
    AddressDto getAddressWarehouse() throws FeignException;

    // 22
    @PostMapping("/shipped")
    void shippedToDelivery(@Valid @RequestBody ShippedToDeliveryRequest request) throws FeignException;

    @PostMapping("/return")
    void acceptReturnToWarehouse(@RequestBody Map<UUID, Long> returnedProducts) throws FeignException;

    @PostMapping("/assembly")
    BookedProductsDto assemblyProductsForOrder(@Valid @RequestBody
                                               AssemblyProductsForOrderRequest request) throws FeignException;
}
