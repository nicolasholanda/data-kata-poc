package com.github.nicolasholanda.ingestion.jdbc;

public record JdbcConfig(String url, String user, String password) {

    public static JdbcConfig fromEnv() {
        return new JdbcConfig(
            System.getenv("SOURCE_DB_URL"),
            System.getenv("SOURCE_DB_USER"),
            System.getenv("SOURCE_DB_PASSWORD")
        );
    }
}
