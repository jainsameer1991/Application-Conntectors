package com.connector.hbase;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Slf4j
@Path("/hbase")
public class HBaseResource {

    private static ObjectMapper objectMapper;

    @GET
    @Path("/get-data")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getData() {

        start();
        String data = "FirstString";
        Response.ResponseBuilder responseBuilder = Response.status(Response.Status.OK);
        return responseBuilder.entity(data).build();
    }


    public void connectToHBase() {

        Configuration config = HBaseConfiguration.create();

        String path = this.getClass()
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        config.set("hbase.zookeeper.quorum", "127.0.0.1");
        config.set("zookeeper.znode.parent", "/hbase");
//        config.set("zookeeper.znode.parent", "/hbase-unsecure");

        config.set("hbase.zookeeper.property.clientPort","2181");
        config.set("hbase.master", "172.17.0.2:60000");
//        config.addResource(new org.apache.hadoop.fs.Path(path));
        try {
            Connection connection = ConnectionFactory.createConnection(config);
            Table customers = connection.getTable(TableName.valueOf("customers"));
            Result result = customers.get(new Get(Bytes.toBytes("1")));
            System.out.println(serialise(result));


        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public static String serialise(Object data) {
        try {
            return getObjectMapper().writeValueAsString(data);
        } catch (IOException e) {
            throw new RuntimeException("Not able to serialise : " + data, e);
        }
    }



    public static ObjectMapper getObjectMapper() {
        return getObjectMapper(false);
    }

    public static ObjectMapper getObjectMapper(boolean createNewInstance) {

        if (objectMapper != null && !createNewInstance)
            return objectMapper;

        ObjectMapper objectMapperInstance = new ObjectMapper();
        enrichObjectMapper(objectMapperInstance);
        // newInstance request should not override global ObjectMapper instance
        if (!createNewInstance)
            objectMapper = objectMapperInstance;

        return objectMapperInstance;
    }

    public static void enrichObjectMapper(ObjectMapper objectMapperInstance) {
        objectMapperInstance.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
        objectMapperInstance.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapperInstance.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        objectMapperInstance.registerModule(new Jdk8Module());
        objectMapperInstance.setSubtypeResolver(new StdSubtypeResolver());
        objectMapperInstance.setTimeZone(TimeZone.getTimeZone("Asia/Kolkata"));
    }

    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        config.clear();
        config.setInt("timeout", 12000);
        config.set("zookeeper.znode.parent", "/hbase");
        config.set("hbase.zookeeper.quorum", "172.17.0.2");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master", "172.17.0.2:60010");

//        String path = HBaseResource.class
//                .getClassLoader()
//                .getResource("/Users/sameer.jain/repo/ApplicationConnector/Application Connector/Application Connectors/application-connectors-service/src/main/resources/hbase-site.xml")
//                .getPath();
//        config.addResource(new org.apache.hadoop.fs.Path(path));
        try {
            HBaseAdmin.available(config);
            System.out.println("Connected");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        try {
            TTransport socket = new TSocket("localhost", 9090);// IP Host Name
            TProtocol protocol = new TBinaryProtocol(socket, true, true);// Note here
            Hbase.Client client = new Hbase.Client(protocol);
            socket.open();
            System.out.println("open");
            try {
                System.out.println("scanning tables...");


                for (ByteBuffer name : client.getTableNames()) {
                    System.out.println(byteBufferToStr(name, Charset.defaultCharset()));
                    HashMap<ByteBuffer, ByteBuffer> attributes = new HashMap<>();
                    List<TRowResult> rows = client.getRow(name, strToByteBuffer("1", Charset.defaultCharset()),
                            attributes);
                    rows
                            .forEach(row -> {
                        Map<ByteBuffer, TCell> columns = row.getColumns();
                        columns
                                .forEach((colKey,colVal) -> {
                                    System.out.printf("key: %s, value: %s%n", byteBufferToStr(colKey, Charset.defaultCharset()), byteBufferToStr(colVal.value, Charset.defaultCharset()));
                                });
                    });
//                    client.getRow()
//                    System.out.println(name);
                    //code
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(e.getMessage());
            }
            socket.close();
            System.out.println("close");
        } catch (TTransportException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (TException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());

        }
    }

    public static ByteBuffer strToByteBuffer(String msg, Charset charset){
        return ByteBuffer.wrap(msg.getBytes(charset));
    }

    public static String byteBufferToStr(ByteBuffer buffer, Charset charset){
//        String s = StandardCharsets.UTF_8.decode(buffer).toString();
        byte[] bytes;
        if(buffer.hasArray()) {
            bytes = buffer.array();
        } else {
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
        }
        return new String(bytes, charset);
    }


}
