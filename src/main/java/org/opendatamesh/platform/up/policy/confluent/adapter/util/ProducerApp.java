package org.opendatamesh.platform.up.policy.confluent.adapter.util;


import com.acme.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.opendatamesh.platform.up.policy.confluent.adapter.rules.ODMPolicyExecutor;
import org.opendatamesh.platform.up.policy.confluent.adapter.rules.ODMRuleAction;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerApp implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ProducerApp.class);

    private String TEST_ORDER_MESSAGE_FILEPATH = "./src/main/resources/order.json";
    private KafkaProducer<String, Object> producer;
    private KafkaProducer<String, Object> dlqProducer;
    private Properties props;
    private String topic;

    public ProducerApp(String propertiesFile, String clientId) {
        try {

            props = ClientUtils.loadConfig(propertiesFile);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            if (clientId != null) {
                props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            }
            props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
            props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
            Properties dlqProps = setUpDlqProducerProperties(props, "client-2");
            dlqProducer = new KafkaProducer<>(dlqProps);

            props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS, "CUSTOMEXECUTOR");
            props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".CUSTOMEXECUTOR.class" , ODMPolicyExecutor.class.getName());
            props.put(AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT, "false");

            props.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS, "CUSTOMACTION"); //verifyPolicy
            props.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".CUSTOMACTION.class",
                    ODMRuleAction.class.getName());


            // Set up local parameters. They aren't exposed in the schema registry
            String username = System.getenv("EMAIL_USER");
            String password = System.getenv("EMAIL_PASS");


            props.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".CUSTOMACTION.param.producer", dlqProducer);
            props.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".CUSTOMACTION.param.username", //verifyPolicy
                    username);
            props.put(AbstractKafkaSchemaSerDeConfig.RULE_ACTIONS + ".CUSTOMACTION.param.password", //verifyPolicy
                    password);

            topic = props.getProperty("topic");


            // Create the topic if it doesn't exist already
            ClientUtils.createTopic(props, topic);

        } catch (Exception e) {
            logger.error("Error in ProducerApp.constructor: " + e);
        }
    }

    @Override
    public void run() {
        try {
            producer = new KafkaProducer<>(props);
            Order order = readOrderFromFile(TEST_ORDER_MESSAGE_FILEPATH);

            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, order);

            try {
                producer.send(record);
                logger.info("New order: " + record.value());
            }catch (RuntimeException e){
                logger.error("ERROR in Order: " + record.value() );
            }
            finally {

            }

        } catch (Exception e) {
            logger.error("Error in ProducerApp.run " + e);
            e.printStackTrace();

        }
    }

    public static void main(final String[] args) {
        logger.info("Starting to produce Orders");
        if (args.length < 2) {
            logger.error("Provide the properties file and client ID as arguments");
            System.exit(1);
        }
        ProducerApp producer = new ProducerApp(args[0], args[1]);
        producer.run();
    }

    private Order readOrderFromFile(String filepath) throws IOException{
        Path path = Paths.get(filepath);
        if (!Files.exists(path)) {
            throw new IOException(filepath + " not found.");
        }
        Order order;
        try (InputStream inputStream = Files.newInputStream(path)) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            order = objectMapper.readValue(inputStream, Order.class);
        }
        return order;
    }

    private Properties setUpDlqProducerProperties(Properties source, String clientId){
        Properties dlqProps = new Properties();
        dlqProps.putAll(props);
        dlqProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        /*dlqProps.remove(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS);
        dlqProps.remove(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION);
        dlqProps.remove(AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT);
        if(dlqProps.contains(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG))
            dlqProps.remove(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
        if(dlqProps.contains(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_USER_INFO_CONFIG))
            dlqProps.remove(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_USER_INFO_CONFIG);
        if(dlqProps.contains(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE))
            dlqProps.remove(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE);*/


        dlqProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        dlqProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return dlqProps;
    }


    // Used to spawn infinite messages using OrderGen class
    private void sendInfiniteMessages() throws Exception{
        while (true) {
            // Create a producer record
            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, OrderGen.getNewOrder());

            // Send the record
            try {
                producer.send(record);

                logger.info("New order: " + record.value());
            }catch (RuntimeException e){
                logger.error("ERROR in Order: " + record.value() );
            }
            finally {
                System.out.println("----------------------------------------------------------");
            }
            Thread.sleep(5000);
        }

    }
}