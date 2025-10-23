package ru.yandex.practicum.error;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.yandex.practicum.exception.*;

@Slf4j
@RestControllerAdvice
public class ErrorHandler {

    @ExceptionHandler
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleNoProductsInShoppingCart(NoProductsInShoppingCartException e) {
        log.error("No products in shopping cart {}", e.getMessage(), e);
        return new ErrorResponse("Нет товаров в корзине покупок", e.getMessage());
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleNoSpecifiedProductInWarehouse(NoSpecifiedProductInWarehouseException e) {
        log.error("No specified product in warehouse {}", e.getMessage(), e);
        return new ErrorResponse("На складе нет указанного товара", e.getMessage());
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    public ErrorResponse handleNotAuthorizedUser(NotAuthorizedUserException e) {
        log.error("User is not authorized {}", e.getMessage(), e);
        return new ErrorResponse("Пользователь не авторизован", e.getMessage());
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleProductInShoppingCartLowQuantityInWarehouse(ProductInShoppingCartLowQuantityInWarehouse e) {
        log.error("The product from the basket is not in the required quantity in warehouse {}", e.getMessage(), e);
        return new ErrorResponse("Товара из корзины нет в необходимом количестве на складе", e.getMessage());
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse handleProductNotFound(ProductNotFoundException e) {
        log.error("Product not found {}", e.getMessage(), e);
        return new ErrorResponse("Товар не найден", e.getMessage());
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleSpecifiedProductAlreadyInWarehouse(SpecifiedProductAlreadyInWarehouseException e) {
        log.error("Product is already in warehouse {}", e.getMessage(), e);
        return new ErrorResponse("Указанный товар уже находится на складе", e.getMessage());
    }
}
