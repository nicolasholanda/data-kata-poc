package com.github.nicolasholanda.api.repository;

import com.github.nicolasholanda.api.dto.TopSalesByCityResponse;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;

@Repository
public class TopSalesByCityRepository {

    private final JdbcClient jdbc;

    public TopSalesByCityRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public List<TopSalesByCityResponse> findAll() {
        return jdbc.sql("SELECT city_name, state, total_sales, period_start, period_end, computed_at " +
                        "FROM top_sales_by_city " +
                        "ORDER BY total_sales DESC")
            .query(TopSalesByCityResponse.class)
            .list();
    }

    public List<TopSalesByCityResponse> findByPeriod(LocalDate periodStart, LocalDate periodEnd) {
        return jdbc.sql("SELECT city_name, state, total_sales, period_start, period_end, computed_at " +
                        "FROM top_sales_by_city WHERE period_start = :periodStart AND period_end = :periodEnd " +
                        "ORDER BY total_sales DESC")
            .param("periodStart", periodStart)
            .param("periodEnd", periodEnd)
            .query(TopSalesByCityResponse.class)
            .list();
    }
}
