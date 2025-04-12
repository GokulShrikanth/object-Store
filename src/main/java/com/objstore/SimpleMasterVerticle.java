package com.objstore;

import com.objstore.common.CacheMessage;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

import java.util.HashMap;
import java.util.Map;

public class SimpleMasterVerticle {

    private static final Map<String, Object> CACHE = new HashMap<>();

    public static void main(String[] args) {
        System.out.println("Starting Simple Master in clustered mode...");

        VertxOptions options = new VertxOptions()
            .setClusterManager(new HazelcastClusterManager());

        options.getEventBusOptions()
            .setHost("localhost")
            .setClusterPublicHost("localhost");

        Vertx.clusteredVertx(options, res -> {
            if (res.succeeded()) {
                Vertx vertx = res.result();

                // Register CacheMessage codec
                CacheMessage.registerCodec(vertx);

                // Deploy simple master handler
                vertx.deployVerticle(new AbstractVerticle() {
                    @Override
                    public void start(Promise<Void> startPromise) {
                        System.out.println("Simple Master verticle started");

                        // Register to receive messages at cache.master address
                        vertx.eventBus().consumer("cache.master", message -> {
                            try {
                                System.out.println("Received message at cache.master");
                                CacheMessage cacheMsg = (CacheMessage) message.body();

                                if ("PUT".equals(cacheMsg.operation)) {
                                    CACHE.put(cacheMsg.key, cacheMsg.value);
                                    System.out.println("PUT key=" + cacheMsg.key + " value=" + cacheMsg.value);
                                    message.reply("OK");
                                } else if ("GET".equals(cacheMsg.operation)) {
                                    Object value = CACHE.get(cacheMsg.key);
                                    System.out.println("GET key=" + cacheMsg.key + " value=" + value);
                                    message.reply(value != null ? value : "Key not found");
                                } else {
                                    message.fail(400, "Unknown operation: " + cacheMsg.operation);
                                }
                            } catch (Exception e) {
                                System.err.println("Error processing message: " + e);
                                message.fail(500, e.getMessage());
                            }
                        });

                        System.out.println("Registered handler at address: cache.master");
                        startPromise.complete();
                    }
                }, ar -> {
                    if (ar.succeeded()) {
                        System.out.println("Simple Master deployed successfully");
                    } else {
                        System.err.println("Failed to deploy Simple Master: " + ar.cause());
                    }
                });
            } else {
                System.err.println("Failed to create clustered Vert.x: " + res.cause());
            }
        });

        // Keep main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
