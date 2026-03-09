package com.github.nicolasholanda.api.controller;

import com.github.nicolasholanda.api.dto.TopSalesByCityResponse;
import com.github.nicolasholanda.api.repository.TopSalesByCityRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.List;

@RestController
@RequestMapping("/api/top-sales-by-city")
public class TopSalesByCityController {

    private final TopSalesByCityRepository repository;

    public TopSalesByCityController(TopSalesByCityRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public List<TopSalesByCityResponse> get(
            @RequestParam(required = false) LocalDate periodStart,
            @RequestParam(required = false) LocalDate periodEnd) {
        if (periodStart != null && periodEnd != null) {
            return repository.findByPeriod(periodStart, periodEnd);
        }
        return repository.findAll();
    }
}
