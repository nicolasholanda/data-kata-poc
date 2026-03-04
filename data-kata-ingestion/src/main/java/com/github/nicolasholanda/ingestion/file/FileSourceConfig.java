package com.github.nicolasholanda.ingestion.file;

public record FileSourceConfig(String citiesPath, String productsPath) {

    public static FileSourceConfig fromEnv() {
        return new FileSourceConfig(
            System.getenv("CITIES_FILE_PATH"),
            System.getenv("PRODUCTS_FILE_PATH")
        );
    }
}
