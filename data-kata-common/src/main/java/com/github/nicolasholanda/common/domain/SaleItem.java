package com.github.nicolasholanda.common.domain;

import java.math.BigDecimal;

public record SaleItem(long id, long saleId, long productId, int quantity, BigDecimal unitPrice) {
}
