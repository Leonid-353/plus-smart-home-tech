package ru.yandex.practicum.feign;

import feign.FeignException;
import jakarta.validation.Valid;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

@FeignClient(name = "shopping-cart", path = "/api/v1/shopping-cart")
public interface CartFeignClient {

    // Create
    @PutMapping
    ShoppingCartDto addProductInCart(@RequestParam String username,
                                     @RequestBody Map<UUID, Long> products) throws FeignException;

    // Read
    @GetMapping
    ShoppingCartDto getCartForUser(@RequestParam String username) throws FeignException;

    // Update
    @PostMapping("/remove")
    ShoppingCartDto removeProductFromCart(@RequestParam String username,
                                          @RequestBody Set<UUID> productIds) throws FeignException;

    @PostMapping("/change-quantity")
    ShoppingCartDto changeProductQuantity(@RequestParam String username,
                                          @Valid @RequestBody ChangeProductQuantityRequest request) throws FeignException;

    // Delete
    @DeleteMapping
    void deactivateUserCart(@RequestParam String username) throws FeignException;
}
