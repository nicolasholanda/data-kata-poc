package com.github.nicolasholanda.api.controller;

import com.github.nicolasholanda.api.dto.TopSalesmanResponse;
import com.github.nicolasholanda.api.repository.TopSalesmanRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.List;

@RestController
@RequestMapping("/api/top-salesmen")
public class TopSalesmanController {

    private final TopSalesmanRepository repository;

    public TopSalesmanController(TopSalesmanRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public List<TopSalesmanResponse> get(
            @RequestParam(required = false) LocalDate periodStart,
            @RequestParam(required = false) LocalDate periodEnd) {
        if (periodStart != null && periodEnd != null) {
            return repository.findByPeriod(periodStart, periodEnd);
        }
        return repository.findAll();
    }
}
