package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;
import lombok.experimental.FieldDefaults;
import ru.yandex.practicum.dto.store.enums.ProductCategory;
import ru.yandex.practicum.dto.store.enums.ProductState;
import ru.yandex.practicum.dto.store.enums.QuantityState;

import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "products")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class Product {
    @Id
    @Column(name = "product_id")
    @GeneratedValue(strategy = GenerationType.UUID)
    UUID productId;

    @Column(name = "product_name", nullable = false)
    String productName;

    @Column(name = "description", length = 2000, nullable = false)
    String description;

    @Column(name = "image_src", length = 500)
    String imageSrc;

    @Enumerated(EnumType.STRING)
    @Column(name = "product_category", length = 50)
    ProductCategory productCategory;

    @Enumerated(EnumType.STRING)
    @Column(name = "product_state", length = 50, nullable = false)
    ProductState productState = ProductState.ACTIVE;

    @Enumerated(EnumType.STRING)
    @Column(name = "quantity_state", length = 50, nullable = false)
    QuantityState quantityState;

    @Column(name = "price")
    BigDecimal price;
}
