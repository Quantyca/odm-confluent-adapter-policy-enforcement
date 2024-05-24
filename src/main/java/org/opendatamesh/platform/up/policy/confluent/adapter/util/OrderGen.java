package org.opendatamesh.platform.up.policy.confluent.adapter.util;

import com.acme.Order;
import com.acme.OrderStatus;
import com.github.javafaker.Faker;
import java.time.Instant;

public class OrderGen {

    public static Order getNewOrder() {
        Faker fake = new Faker();
        return new Order(
                fake.number().numberBetween(1, 10000),
                fake.number().numberBetween(1, 10000),
                fake.number().numberBetween(1, 1000),
                OrderStatus.Processing,
                Instant.now()
        );
    }
}