package ru.yandex.practicum.service;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.exception.NoProductsInShoppingCartException;
import ru.yandex.practicum.exception.NotAuthorizedUserException;
import ru.yandex.practicum.mapper.CartMapper;
import ru.yandex.practicum.model.Cart;
import ru.yandex.practicum.repository.CartRepository;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class CartServiceImpl implements CartService {
    final CartRepository cartRepository;
    final CartMapper cartMapper;

    // Create
    @Override
    @Transactional
    public ShoppingCartDto addProductInCart(String username, Map<UUID, Long> products) {
        validateUsername(username);

        if (products == null || products.isEmpty()) {
            throw new IllegalArgumentException("Список товаров не может быть пустым");
        }

        Cart cart = cartRepository.findByUsername(username)
                .orElseGet(() -> createNewCart(username));

        products.forEach((productId, quantity) -> {
            validateQuantity(quantity);
            cart.getProducts().merge(productId, quantity, Long::sum);
        });

        Cart savedCart = cartRepository.save(cart);
        log.info("Добавлены товары в корзину пользователя: {}", username);
        return cartMapper.convertToDto(savedCart);
    }

    // Read
    @Override
    @Transactional(readOnly = true)
    public ShoppingCartDto getCartForUser(String username) {
        validateUsername(username);

        return cartRepository.findByUsername(username)
                .map(cartMapper::convertToDto)
                .orElseGet(() -> {
                    log.debug("Создана новая корзина для пользователя: {}", username);
                    return createNewCartDto(username);
                });
    }


    // Update
    @Override
    @Transactional
    public ShoppingCartDto removeProductFromCart(String username, Set<UUID> productIds) {
        validateUsername(username);
        validateProductIds(productIds);

        Cart cart = cartRepository.findByUsername(username)
                .orElseThrow(() -> new NoProductsInShoppingCartException(
                        "Корзина не найдена для пользователя: " + username));

        Set<UUID> missingProductIds = productIds.stream()
                .filter(productId -> !cart.getProducts().containsKey(productId))
                .collect(Collectors.toSet());

        if (!missingProductIds.isEmpty()) {
            throw new NoProductsInShoppingCartException(
                    "Товары не найдены в корзине: " + missingProductIds);
        }

        productIds.forEach(cart.getProducts()::remove);

        Cart savedCart = cartRepository.save(cart);
        log.info("Удалены товары {} из корзины пользователя: {}", productIds, username);
        return cartMapper.convertToDto(savedCart);
    }

    @Override
    @Transactional
    public ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request) {
        validateUsername(username);

        Cart cart = cartRepository.findByUsername(username)
                .orElseThrow(() -> new NoProductsInShoppingCartException(
                        "Корзина не найдена для пользователя: " + username));

        UUID productId = request.getProductId();
        if (!cart.getProducts().containsKey(productId)) {
            throw new NoProductsInShoppingCartException(
                    "Товар не найден в корзине: " + productId);
        }

        cart.getProducts().put(productId, request.getNewQuantity());

        Cart savedCart = cartRepository.save(cart);
        log.info("Изменено количество товара {} в корзине пользователя: {}", productId, username);
        return cartMapper.convertToDto(savedCart);
    }

    // Delete
    @Override
    @Transactional
    public void deactivateUserCart(String username) {
        validateUsername(username);

        Cart cart = cartRepository.findByUsername(username)
                .orElseThrow(() -> new NoProductsInShoppingCartException(
                        "Корзина не найдена для пользователя: " + username));

        cart.setIsActive(false);
        cartRepository.save(cart);

        log.info("Корзина пользователя {} деактивирована", username);
    }

    // Validation methods. Methods create new cart.
    private void validateUsername(String username) {
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Имя пользователя не должно быть пустым");
        }
    }

    private void validateQuantity(Long quantity) {
        if (quantity == null || quantity <= 0) {
            throw new IllegalArgumentException("Количество товара должно быть положительным числом");
        }
    }

    private void validateProductIds(Set<UUID> productIds) {
        if (productIds == null || productIds.isEmpty()) {
            throw new IllegalArgumentException("Список ID товаров не может быть пустым");
        }
    }

    private ShoppingCartDto createNewCartDto(String username) {
        Cart newCart = createNewCart(username);
        return cartMapper.convertToDto(newCart);
    }

    private Cart createNewCart(String username) {
        Cart newCart = Cart.builder()
                .username(username)
                .isActive(true)
                .build();
        return cartRepository.save(newCart);
    }
}
