package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;
import ru.yandex.practicum.dto.delivery.DeliveryDto;
import ru.yandex.practicum.model.Delivery;

@Mapper(componentModel = "spring")
public interface DeliveryMapper {
    Delivery convertToEntity(DeliveryDto dto);

    DeliveryDto convertToDto(Delivery delivery);
}
