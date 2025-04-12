package com.objstore;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

public class MainApp {
    public static void main(String[] args) {
        System.out.println("Starting Vert.x in clustered mode...");

        // Create a Hazelcast cluster manager
        HazelcastClusterManager clusterManager = new HazelcastClusterManager();

        // Configure Vert.x options
        VertxOptions options = new VertxOptions()
            .setClusterManager(clusterManager);

        // Configure event bus options
        options.getEventBusOptions()
            .setHost("localhost")
            .setClusterPublicHost("localhost");

        // Create clustered Vert.x instance
        Vertx.clusteredVertx(options, res -> {
            if (res.succeeded()) {
                Vertx vertx = res.result();
                // Register codec
                com.objstore.common.CacheMessage.registerCodec(vertx);

                System.out.println("Clustered Vert.x created successfully");

                if (args.length > 0 && "SLAVE".equals(args[0])) {
                    // Deploy as a slave with address from args[1]
                    String slaveAddress = args.length > 1 ? args[1] : "slave-" + System.currentTimeMillis() % 10000;

                    JsonObject config = new JsonObject().put("slaveAddress", slaveAddress);
                    DeploymentOptions deployOptions = new DeploymentOptions().setConfig(config);

                    vertx.deployVerticle("com.objstore.slave.SlaveVerticle", deployOptions, ar -> {
                        if (ar.succeeded()) {
                            System.out.println("Slave deployed successfully with address: " + slaveAddress);
                        } else {
                            System.err.println("Failed to deploy slave: " + ar.cause());
                        }
                    });
                } else {
                    // Deploy as master
                    vertx.deployVerticle("com.objstore.master.MasterVerticle", ar -> {
                        if (ar.succeeded()) {
                            System.out.println("Master deployed successfully");
                        } else {
                            System.err.println("Failed to deploy master: " + ar.cause());
                        }
                    });
                }
            } else {
                System.err.println("Failed to create clustered Vert.x: " + res.cause());
            }
        });

        // Keep the main thread from exiting
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            System.err.println("Main thread interrupted: " + e.getMessage());
        }
    }
}
