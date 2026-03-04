package com.github.nicolasholanda.ingestion.soap;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.transform.Source;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import java.util.ArrayList;
import java.util.List;

public class SalesmanRowMapper {

    public static final StructType SCHEMA = new StructType()
        .add("id", DataTypes.LongType)
        .add("name", DataTypes.StringType);

    public List<Row> toRows(Source source) throws Exception {
        DOMResult result = new DOMResult();
        TransformerFactory.newInstance().newTransformer().transform(source, result);
        NodeList returns = ((Document) result.getNode()).getElementsByTagName("return");
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < returns.getLength(); i++) {
            rows.add(toRow((Element) returns.item(i)));
        }
        return rows;
    }

    private Row toRow(Element element) {
        long id = Long.parseLong(element.getElementsByTagName("id").item(0).getTextContent());
        String name = element.getElementsByTagName("name").item(0).getTextContent();
        return RowFactory.create(id, name);
    }
}
