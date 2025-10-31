package ru.yandex.practicum.feign;

import feign.FeignException;
import jakarta.validation.Valid;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.store.ProductDto;
import ru.yandex.practicum.dto.store.SetProductQuantityStateRequest;
import ru.yandex.practicum.dto.store.enums.ProductCategory;

import java.util.UUID;

@FeignClient(name = "shopping-store", path = "/api/v1/shopping-store")
public interface StoreFeignClient {

    // Create
    @PutMapping
    ProductDto createProduct(@Valid @RequestBody ProductDto dto) throws FeignException;

    // Read
    @GetMapping
    Page<ProductDto> getProducts(@RequestParam ProductCategory category,
                                 @PageableDefault(size = 10) Pageable pageable) throws FeignException;

    @GetMapping("/{productId}")
    ProductDto getProduct(@PathVariable UUID productId) throws FeignException;

    // Update
    @PostMapping
    ProductDto updateProduct(@Valid @RequestBody ProductDto productDto) throws FeignException;

    @PostMapping("/quantityState")
    Boolean updateQuantityState(@Valid /*@RequestBody*/ SetProductQuantityStateRequest request) throws FeignException;

    // Delete
    @PostMapping("/removeProductFromStore")
    Boolean deactivateProduct(@RequestBody UUID productId) throws FeignException;

}
