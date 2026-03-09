package com.github.nicolasholanda.api.repository;

import com.github.nicolasholanda.api.dto.TopSalesmanResponse;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;

@Repository
public class TopSalesmanRepository {

    private final JdbcClient jdbc;

    public TopSalesmanRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public List<TopSalesmanResponse> findAll() {
        return jdbc.sql("SELECT salesman_name, total_sales, period_start, period_end, computed_at " +
                        "FROM top_salesmen " +
                        "ORDER BY total_sales DESC")
            .query(TopSalesmanResponse.class)
            .list();
    }

    public List<TopSalesmanResponse> findByPeriod(LocalDate periodStart, LocalDate periodEnd) {
        return jdbc.sql("SELECT salesman_name, total_sales, period_start, period_end, computed_at " +
                        "FROM top_salesmen WHERE period_start = :periodStart AND period_end = :periodEnd " +
                        "ORDER BY total_sales DESC")
            .param("periodStart", periodStart)
            .param("periodEnd", periodEnd)
            .query(TopSalesmanResponse.class)
            .list();
    }
}
