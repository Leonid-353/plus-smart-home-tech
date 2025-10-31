package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.MappingTarget;
import ru.yandex.practicum.dto.cart.ShoppingCartDto;
import ru.yandex.practicum.model.Cart;

@Mapper(componentModel = "spring")
public interface CartMapper {
    ShoppingCartDto convertToDto(Cart cart);

    Cart convertToEntity(ShoppingCartDto dto);

    void updateEntityFromDto(ShoppingCartDto dto, @MappingTarget Cart cart);
}
