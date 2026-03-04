package com.github.nicolasholanda.common.domain;

import java.time.LocalDate;

public record Sale(long id, long salesmanId, long cityId, LocalDate date) {
}
