package ru.yandex.practicum.service;

import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface CartService {

    // Create
    ShoppingCartDto addProductInCart(String username, Map<UUID, Long> products);

    // Read
    ShoppingCartDto getCartForUser(String username);

    // Update
    ShoppingCartDto removeProductFromCart(String username, Set<UUID> productIds);

    ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request);

    // Delete
    void deactivateUserCart(String username);
}
