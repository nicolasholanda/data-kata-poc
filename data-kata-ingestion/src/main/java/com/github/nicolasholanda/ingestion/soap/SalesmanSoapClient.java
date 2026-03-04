package com.github.nicolasholanda.ingestion.soap;

import jakarta.xml.ws.Dispatch;
import jakarta.xml.ws.Service;
import jakarta.xml.ws.soap.SOAPBinding;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.namespace.QName;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;

public class SalesmanSoapClient {

    private final SparkSession spark;
    private final SalesmanSoapConfig config;
    private final SalesmanRowMapper mapper;

    public SalesmanSoapClient(SparkSession spark, SalesmanSoapConfig config) {
        this.spark = spark;
        this.config = config;
        this.mapper = new SalesmanRowMapper();
    }

    public Dataset<Row> readSalesmen() {
        try {
            String payload = "<ns:getAllSalesmen xmlns:ns=\"" + config.namespace() + "\"/>";
            Source response = buildDispatch().invoke(new StreamSource(new StringReader(payload)));
            return spark.createDataFrame(mapper.toRows(response), SalesmanRowMapper.SCHEMA);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read salesmen from SOAP service", e);
        }
    }

    private Dispatch<Source> buildDispatch() {
        QName serviceName = new QName(config.namespace(), config.serviceName());
        QName portName = new QName(config.namespace(), config.portName());
        Service service = Service.create(serviceName);
        service.addPort(portName, SOAPBinding.SOAP11HTTP_BINDING, config.serviceUrl());
        return service.createDispatch(portName, Source.class, Service.Mode.PAYLOAD);
    }
}
