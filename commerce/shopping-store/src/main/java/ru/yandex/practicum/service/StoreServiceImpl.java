package ru.yandex.practicum.service;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.store.ProductDto;
import ru.yandex.practicum.dto.store.SetProductQuantityStateRequest;
import ru.yandex.practicum.dto.store.enums.ProductCategory;
import ru.yandex.practicum.dto.store.enums.ProductState;
import ru.yandex.practicum.exception.ProductNotFoundException;
import ru.yandex.practicum.mapper.StoreMapper;
import ru.yandex.practicum.model.Product;
import ru.yandex.practicum.repository.StoreRepository;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class StoreServiceImpl implements StoreService {
    final StoreRepository storeRepository;
    final StoreMapper storeMapper;

    // Create
    @Override
    @Transactional
    public ProductDto createProduct(ProductDto productDto) {
        Product product = storeMapper.convertToEntity(productDto);
        return storeMapper.convertToDto(storeRepository.save(product));
    }

    // Read
    @Override
    public Page<ProductDto> getProducts(ProductCategory category, Pageable pageable) {
        return storeRepository.findByProductCategory(category, pageable)
                .map(storeMapper::convertToDto);
    }

    @Override
    public ProductDto getProduct(UUID productId) {
        return storeRepository.findById(productId)
                .map(storeMapper::convertToDto)
                .orElseThrow(() -> new ProductNotFoundException("Товар id: " + productId + " не найден"));
    }

    // Update
    @Override
    public ProductDto updateProduct(ProductDto productDto) {
        Product existingProduct = storeRepository.findById(productDto.getProductId())
                .orElseThrow(() -> new ProductNotFoundException(
                        "Товар id: " + productDto.getProductId() + " не найден")
                );
        storeMapper.updateEntityFromDto(productDto, existingProduct);
        return storeMapper.convertToDto(storeRepository.save(existingProduct));
    }

    @Override
    public Boolean updateQuantityState(SetProductQuantityStateRequest request) {
        Product product = storeRepository.findById(request.getProductId())
                .orElseThrow(() -> new ProductNotFoundException(
                        "Товар id: " + request.getProductId() + " не найден")
                );
        product.setQuantityState(request.getQuantityState());
        storeRepository.save(product);
        return true;
    }

    // Delete
    @Override
    public Boolean deactivateProduct(UUID productId) {
        Product product = storeRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundException(
                        "Товар id: " + productId + " не найден")
                );
        product.setProductState(ProductState.DEACTIVATE);
        storeRepository.save(product);
        return true;
    }
}
