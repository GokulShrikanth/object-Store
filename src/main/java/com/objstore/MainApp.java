package com.objstore;

import io.vertx.core.Vertx;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;

public class MainApp {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        // Register the message codec
        com.objstore.common.CacheMessage.registerCodec(vertx);

        if (args.length > 0 && "SLAVE".equals(args[0])) {
            // Deploy as a slave with address from args[1]
            String slaveAddress = args.length > 1 ? args[1] : "slave-" + System.currentTimeMillis() % 10000;

            JsonObject config = new JsonObject().put("slaveAddress", slaveAddress);
            DeploymentOptions options = new DeploymentOptions().setConfig(config);

            vertx.deployVerticle("com.objstore.slave.SlaveVerticle", options, ar -> {
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
                    deployInitialSlave(vertx);
                } else {
                    System.err.println("Failed to deploy master: " + ar.cause());
                }
            });
        }
    }

    private static void deployInitialSlave(Vertx vertx) {
        String slaveAddress = "slave-initial";
        JsonObject config = new JsonObject().put("slaveAddress", slaveAddress);
        DeploymentOptions options = new DeploymentOptions().setConfig(config);

        vertx.deployVerticle("com.objstore.slave.SlaveVerticle", options, ar -> {
            if (ar.succeeded()) {
                System.out.println("Initial slave deployed with address: " + slaveAddress);
            } else {
                System.err.println("Failed to deploy initial slave: " + ar.cause());
            }
        });
    }
}
