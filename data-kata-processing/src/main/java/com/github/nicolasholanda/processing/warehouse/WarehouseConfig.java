package com.github.nicolasholanda.processing.warehouse;

public record WarehouseConfig(String url, String user, String password) {

    public static WarehouseConfig fromEnv() {
        return new WarehouseConfig(
            System.getenv("WAREHOUSE_URL"),
            System.getenv("WAREHOUSE_USER"),
            System.getenv("WAREHOUSE_PASSWORD")
        );
    }
}
