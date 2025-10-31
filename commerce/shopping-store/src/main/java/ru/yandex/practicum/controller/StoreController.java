package ru.yandex.practicum.controller;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.dto.store.ProductDto;
import ru.yandex.practicum.dto.store.SetProductQuantityStateRequest;
import ru.yandex.practicum.dto.store.enums.ProductCategory;
import ru.yandex.practicum.feign.StoreFeignClient;
import ru.yandex.practicum.service.StoreService;

import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-store")
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class StoreController implements StoreFeignClient {
    final StoreService storeService;

    // Create
    @Override
    public ProductDto createProduct(ProductDto productDto) {
        log.info("Получен запрос на создание товара: {}", productDto.getProductName());
        return storeService.createProduct(productDto);
    }

    // Read
    @Override
    public Page<ProductDto> getProducts(ProductCategory category,
                                        Pageable pageable) {
        log.info("Получен запрос на чтение товаров по категории: {}", category);
        return storeService.getProducts(category, pageable);
    }

    @Override
    public ProductDto getProduct(UUID productId) {
        log.info("Получен запрос на чтение товара по id: {}", productId);
        return storeService.getProduct(productId);
    }

    // Update
    @Override
    public ProductDto updateProduct(ProductDto productDto) {
        log.info("Получен запрос на обновление товара id: {}, name: {}",
                productDto.getProductId(),
                productDto.getProductName());
        return storeService.updateProduct(productDto);
    }

    @Override
    public Boolean updateQuantityState(SetProductQuantityStateRequest request) {
        log.info("Получен запрос на обновление количества товара id: {}", request.getProductId());
        return storeService.updateQuantityState(request);
    }

    // Delete
    @Override
    public Boolean deactivateProduct(UUID productId) {
        log.info("Получен запрос на удаление товара id: {}", productId);
        return storeService.deactivateProduct(productId);
    }
}
