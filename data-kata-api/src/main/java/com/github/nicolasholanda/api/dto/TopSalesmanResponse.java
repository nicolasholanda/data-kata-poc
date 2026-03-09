package com.github.nicolasholanda.api.dto;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

public record TopSalesmanResponse(
    String salesmanName,
    BigDecimal totalSales,
    LocalDate periodStart,
    LocalDate periodEnd,
    LocalDateTime computedAt
) {}
