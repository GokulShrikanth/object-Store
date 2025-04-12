package com.objstore;

import com.objstore.common.CacheMessage;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

public class test {
    public static void main(String[] args) {
        // Configure Vert.x to use clustering
        VertxOptions options = new VertxOptions()
            .setClusterManager(new io.vertx.spi.cluster.hazelcast.HazelcastClusterManager());

        // Configure event bus for clustering
        options.getEventBusOptions().setHost("localhost");
        options.getEventBusOptions().setClusterPublicHost("localhost");

        System.out.println("Starting test client in clustered mode...");

        // Create a clustered Vert.x instance
        io.vertx.core.Vertx.clusteredVertx(options, res -> {
            if (res.succeeded()) {
                Vertx vertx = res.result();

                // Register codec for CacheMessage
                CacheMessage.registerCodec(vertx);
                System.out.println("Clustered Vert.x created successfully");

                // Create a message to send
                CacheMessage message = new CacheMessage();
                message.operation = "PUT";
                message.key = "test-key";
                message.value = "Hello from test client!";

                System.out.println("Sending PUT request to cache.master...");

                // Send the message to cache.master
                vertx.eventBus().request("cache.master", message, reply -> {
                    if (reply.succeeded()) {
                        System.out.println("PUT success: " + reply.result().body());
                    } else {
                        System.err.println("PUT failed: " + reply.cause().getMessage());
                    }

                    // Give time for message to be processed before shutting down
                    vertx.setTimer(2000, id -> {
                        System.out.println("Shutting down test client...");
                        vertx.close();
                    });
                });

                // Also do a GET to verify the value was stored
                vertx.setTimer(1000, id -> {
                    CacheMessage getMsg = new CacheMessage();
                    getMsg.operation = "GET";
                    getMsg.key = "test-key";

                    System.out.println("Sending GET request to cache.master...");

                    vertx.eventBus().request("cache.master", getMsg, reply -> {
                        if (reply.succeeded()) {
                            System.out.println("GET result: " + reply.result().body());
                        } else {
                            System.err.println("GET failed: " + reply.cause().getMessage());
                        }
                    });
                });
            } else {
                System.err.println("Failed to create clustered Vert.x: " + res.cause());
            }
        });

        // Keep the main thread alive
        try {
            Thread.sleep(10000); // Wait for operations to complete
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
