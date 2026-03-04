package com.github.nicolasholanda.soapmock;

import jakarta.jws.WebService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@WebService(endpointInterface = "com.github.nicolasholanda.soapmock.SalesmanService")
public class SalesmanServiceImpl implements SalesmanService {

    private volatile List<Salesman> salesmen = new ArrayList<>();

    @Override
    public List<Salesman> getAllSalesmen() {
        return Collections.unmodifiableList(salesmen);
    }

    public void generate(int count) {
        List<Salesman> generated = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            generated.add(new Salesman(i, "Salesman " + i));
        }
        salesmen = generated;
    }
}
