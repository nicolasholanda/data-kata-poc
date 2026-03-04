package com.github.nicolasholanda.common;

public class Columns {

    public static class Sales {
        public static final String ID = "id";
        public static final String SALESMAN_ID = "salesman_id";
        public static final String CITY_ID = "city_id";
        public static final String DATE = "date";
    }

    public static class SaleItems {
        public static final String ID = "id";
        public static final String SALE_ID = "sale_id";
        public static final String PRODUCT_ID = "product_id";
        public static final String QUANTITY = "quantity";
        public static final String UNIT_PRICE = "unit_price";
    }

    public static class Cities {
        public static final String ID = "id";
        public static final String NAME = "name";
        public static final String STATE = "state";
    }

    public static class Products {
        public static final String ID = "id";
        public static final String NAME = "name";
        public static final String CATEGORY = "category";
        public static final String BASE_PRICE = "base_price";
    }

    public static class Salesmen {
        public static final String ID = "id";
        public static final String NAME = "name";
    }

    public static class Results {
        public static final String PERIOD_START = "period_start";
        public static final String PERIOD_END = "period_end";
        public static final String COMPUTED_AT = "computed_at";
    }

    public static class FactSales {
        public static final String ID = "id";
        public static final String SALE_ID = "sale_id";
        public static final String SALESMAN_ID = "salesman_id";
        public static final String CITY_ID = "city_id";
        public static final String PRODUCT_ID = "product_id";
        public static final String QUANTITY = "quantity";
        public static final String UNIT_PRICE = "unit_price";
        public static final String LINE_TOTAL = "line_total";
        public static final String DATE = "date";
    }
}
