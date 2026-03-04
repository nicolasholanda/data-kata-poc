CREATE TABLE dim_city (
    id    BIGINT PRIMARY KEY,
    name  VARCHAR(255) NOT NULL,
    state VARCHAR(2)   NOT NULL
);

CREATE TABLE dim_salesman (
    id   BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE dim_product (
    id         BIGINT PRIMARY KEY,
    name       VARCHAR(255)   NOT NULL,
    category   VARCHAR(255)   NOT NULL,
    base_price NUMERIC(10, 2) NOT NULL
);

CREATE TABLE fact_sales (
    id          BIGINT         PRIMARY KEY,
    sale_id     BIGINT         NOT NULL,
    salesman_id BIGINT         NOT NULL REFERENCES dim_salesman(id),
    city_id     BIGINT         NOT NULL REFERENCES dim_city(id),
    product_id  BIGINT         NOT NULL REFERENCES dim_product(id),
    quantity    INT            NOT NULL,
    unit_price  NUMERIC(10, 2) NOT NULL,
    line_total  NUMERIC(10, 2) NOT NULL,
    date        DATE           NOT NULL
);

CREATE TABLE top_sales_by_city (
    city_name    VARCHAR(255)   NOT NULL,
    state        VARCHAR(2)     NOT NULL,
    total_sales  NUMERIC(14, 2) NOT NULL,
    period_start DATE           NOT NULL,
    period_end   DATE           NOT NULL,
    computed_at  TIMESTAMP      NOT NULL DEFAULT now()
);

CREATE TABLE top_salesmen (
    salesman_name VARCHAR(255)   NOT NULL,
    total_sales   NUMERIC(14, 2) NOT NULL,
    period_start  DATE           NOT NULL,
    period_end    DATE           NOT NULL,
    computed_at   TIMESTAMP      NOT NULL DEFAULT now()
);
