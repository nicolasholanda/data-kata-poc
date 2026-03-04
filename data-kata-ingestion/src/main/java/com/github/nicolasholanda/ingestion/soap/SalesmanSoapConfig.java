package com.github.nicolasholanda.ingestion.soap;

public record SalesmanSoapConfig(String serviceUrl, String namespace, String serviceName, String portName) {

    public static SalesmanSoapConfig fromEnv() {
        return new SalesmanSoapConfig(
            System.getenv("SOAP_SERVICE_URL"),
            System.getenv("SOAP_NAMESPACE"),
            System.getenv("SOAP_SERVICE_NAME"),
            System.getenv("SOAP_PORT_NAME")
        );
    }
}
