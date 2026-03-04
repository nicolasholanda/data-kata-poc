package com.github.nicolasholanda.soapmock;

import jakarta.jws.WebMethod;
import jakarta.jws.WebService;

import java.util.List;

@WebService
public interface SalesmanService {

    @WebMethod
    List<Salesman> getAllSalesmen();
}
