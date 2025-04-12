package com.objstore;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;

public class MainApp {
    public static void main(String[] args) {
        // Configure Vert.x to use clustering
        VertxOptions options = new VertxOptions()
            .setClusterManager(new io.vertx.spi.cluster.hazelcast.HazelcastClusterManager());
        options.getEventBusOptions().setHost("localhost");

        // Register message codec
        io.vertx.core.eventbus.EventBusOptions ebOptions = options.getEventBusOptions();
        ebOptions.setClusterPublicHost("localhost");
        options.setEventBusOptions(ebOptions);

        System.out.println("Starting Vert.x in clustered mode...");

        io.vertx.core.Vertx.clusteredVertx(options, res -> {
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
                            // Deploy the first slave
                            //deployInitialSlave(vertx);
                        } else {
                            System.err.println("Failed to deploy master: " + ar.cause());
                        }
                    });
                }
            } else {
                System.err.println("Failed to create clustered Vert.x: " + res.cause());
            }
        });
    }
}
