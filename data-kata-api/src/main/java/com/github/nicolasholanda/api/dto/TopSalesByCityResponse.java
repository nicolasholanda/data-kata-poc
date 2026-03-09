package com.github.nicolasholanda.api.dto;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

public record TopSalesByCityResponse(
    String cityName,
    String state,
    BigDecimal totalSales,
    LocalDate periodStart,
    LocalDate periodEnd,
    LocalDateTime computedAt
) {}
