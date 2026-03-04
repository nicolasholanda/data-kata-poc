package com.github.nicolasholanda.common.domain;

import java.math.BigDecimal;

public record Product(long id, String name, String category, BigDecimal basePrice) {
}
