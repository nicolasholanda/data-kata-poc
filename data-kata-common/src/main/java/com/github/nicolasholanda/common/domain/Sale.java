package com.github.nicolasholanda.common.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

public record Sale(long id, long salesmanId, long cityId, BigDecimal amount, LocalDate date) {
}
