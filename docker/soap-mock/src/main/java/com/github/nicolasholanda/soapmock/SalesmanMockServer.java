package com.github.nicolasholanda.soapmock;

import com.sun.net.httpserver.HttpServer;
import jakarta.xml.ws.Endpoint;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public class SalesmanMockServer {

    public static void main(String[] args) throws IOException {
        SalesmanServiceImpl impl = new SalesmanServiceImpl();

        Endpoint.publish("http://0.0.0.0:8090/salesmanservice", impl);
        System.out.println("SOAP endpoint listening on port 8090");

        HttpServer adminServer = HttpServer.create(new InetSocketAddress(8091), 0);
        adminServer.createContext("/generate", exchange -> {
            int count = parseCount(exchange.getRequestURI().getQuery());
            impl.generate(count);
            String response = "Generated " + count + " salesmen";
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.getResponseBody().close();
        });
        adminServer.start();
        System.out.println("Admin endpoint listening on port 8091");
    }

    private static int parseCount(String query) {
        if (query == null) return 0;
        for (String param : query.split("&")) {
            String[] kv = param.split("=");
            if (kv.length == 2 && kv[0].equals("count")) {
                return Integer.parseInt(kv[1]);
            }
        }
        return 0;
    }
}
