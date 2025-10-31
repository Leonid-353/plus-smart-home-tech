package ru.yandex.practicum.service;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import ru.yandex.practicum.dto.store.ProductDto;
import ru.yandex.practicum.dto.store.SetProductQuantityStateRequest;
import ru.yandex.practicum.dto.store.enums.ProductCategory;

import java.util.UUID;

public interface StoreService {

    // Create
    ProductDto createProduct(ProductDto productDto);

    // Read
    Page<ProductDto> getProducts(ProductCategory category, Pageable pageable);

    ProductDto getProduct(UUID productId);

    // Update
    ProductDto updateProduct(ProductDto productDto);

    Boolean updateQuantityState(SetProductQuantityStateRequest request);

    // Delete
    Boolean deactivateProduct(UUID productId);
}
