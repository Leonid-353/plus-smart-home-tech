package ru.yandex.practicum.controller;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.feign.CartFeignClient;
import ru.yandex.practicum.service.CartService;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-cart")
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class CartController implements CartFeignClient {
    final CartService cartService;

    // Create
    @Override
    public ShoppingCartDto addProductInCart(String username, Map<UUID, Long> products) {
        log.info("Получен запрос на добавление товара в корзину. Пользователь: {} Товары: {}", username, products);
        return cartService.addProductInCart(username, products);
    }

    // Read
    @Override
    public ShoppingCartDto getCartForUser(String username) {
        log.info("Получен запрос на просмотр актуальной корзины для авторизованного пользователя {}.", username);
        return cartService.getCartForUser(username);
    }


    // Update
    @Override
    public ShoppingCartDto removeProductFromCart(String username, Set<UUID> productIds) {
        log.info("Получен запрос на удаление указанных товаров из корзины пользователя {}. Товары: {}",
                username, productIds);
        return cartService.removeProductFromCart(username, productIds);
    }

    @Override
    public ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request) {
        log.info("Получен запрос на изменение количества товаров в корзине. Пользователь: {}", username);
        return cartService.changeProductQuantity(username, request);
    }

    // Delete
    @Override
    public void deactivateUserCart(String username) {
        log.info("Получен запрос на деактивацию корзины товаров для пользователя {}.", username);
        cartService.deactivateUserCart(username);
    }
}
