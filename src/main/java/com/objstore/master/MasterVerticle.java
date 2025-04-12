// package com.objstore.master;

// import io.vertx.core.AbstractVerticle;
// import com.objstore.common.CacheMessage;
// import io.vertx.core.eventbus.Message;

// import java.util.ArrayList;
// import java.util.List;

// public class MasterVerticle extends AbstractVerticle {
//     private List<String> slaves = new ArrayList<>();

//     @Override
//     public void start() {
//         // Register master address handler
//         vertx.eventBus().consumer("cache.master", this::handleCacheRequest);

//         // Register slave registration handler
//         vertx.eventBus().consumer("slave.registration", this::handleSlaveRegistration);

//         System.out.println("Master verticle started and registered at address: cache.master");
//     }

//     private void handleSlaveRegistration(Message<String> message) {
//         String slaveAddress = message.body();
//         slaves.add(slaveAddress);
//         System.out.println("Registered slave: " + slaveAddress);
//         message.reply("Registered");
//     }

//     private void handleCacheRequest(Message<CacheMessage> message) {
//         if (slaves.isEmpty()) {
//             message.fail(404, "No slaves available");
//             return;
//         }

//         // Simple round-robin or hash-based routing
//         CacheMessage request = message.body();
//         String targetSlave = selectSlave(request.key);

//         vertx.eventBus().request(targetSlave, request, reply -> {
//             if (reply.succeeded()) {
//                 message.reply(reply.result().body());
//             } else {
//                 message.fail(500, reply.cause().getMessage());
//             }
//         });
//     }

//     private String selectSlave(String key) {
//         // Simple hash-based routing
//         int index = Math.abs(key.hashCode() % slaves.size());
//         return slaves.get(index);
//     }
// }

// package com.objstore.master;

// import io.vertx.core.AbstractVerticle;
// import com.objstore.common.CacheMessage;
// import io.vertx.core.eventbus.Message;

// import java.util.ArrayList;
// import java.util.List;

// public class MasterVerticle extends AbstractVerticle {
//     private List<String> slaves = new ArrayList<>();

//     @Override
//     public void start() {
//         System.out.println("Master starting at address: cache.master");

//         // Register master address handler - IMPORTANT: This must use the exact same address
//         vertx.eventBus().consumer("cache.master", this::handleCacheRequest);

//         // Register slave registration handler
//         vertx.eventBus().consumer("slave.registration", this::handleSlaveRegistration);

//         System.out.println("Master verticle started and registered at address: cache.master");
//     }

//     private void handleSlaveRegistration(Message<String> message) {
//         String slaveAddress = message.body();
//         slaves.add(slaveAddress);
//         System.out.println("Registered slave: " + slaveAddress);
//         message.reply("Registered");
//     }

//     private void handleCacheRequest(Message<CacheMessage> message) {
//         CacheMessage request = message.body();
//         System.out.println("Received request: " + request.operation + " for key: " + request.key);

//         if (slaves.isEmpty()) {
//             System.out.println("No slaves available, failing request");
//             message.fail(404, "No slaves available");
//             return;
//         }

//         // Simple round-robin or hash-based routing
//         String targetSlave = selectSlave(request.key);
//         System.out.println("Routing request to slave: " + targetSlave);

//         vertx.eventBus().request(targetSlave, request, reply -> {
//             if (reply.succeeded()) {
//                 message.reply(reply.result().body());
//             } else {
//                 System.out.println("Slave request failed: " + reply.cause());
//                 message.fail(500, reply.cause().getMessage());
//             }
//         });
//     }

//     private String selectSlave(String key) {
//         // Simple hash-based routing
//         int index = Math.abs(key.hashCode() % slaves.size());
//         return slaves.get(index);
//     }
// }

package com.objstore.master;

import io.vertx.core.AbstractVerticle;
import com.objstore.common.CacheMessage;
import io.vertx.core.eventbus.Message;

import java.util.ArrayList;
import java.util.List;
import java.io.Serializable;

public class MasterVerticle extends AbstractVerticle {
    private List<String> slaves = new ArrayList<>();

    @Override
    public void start() {
        System.out.println("Master verticle starting...");

        // Register to receive messages at the cache.master address
        vertx.eventBus().consumer("cache.master", message -> {
            System.out.println("Master received message: " + message.body());
            try {
                CacheMessage cm = (CacheMessage) message.body();
                handleCacheRequest(message);
            } catch (Exception e) {
                System.err.println("Error processing message: " + e.getMessage());
                e.printStackTrace();
                message.fail(500, "Internal error: " + e.getMessage());
            }
        });

        // Register slave registration handler
        vertx.eventBus().consumer("slave.registration", message -> {
            String slaveAddress = (String) message.body();
            slaves.add(slaveAddress);
            System.out.println("Registered slave: " + slaveAddress);
            message.reply("Registered");
        });

        System.out.println("Master verticle started and registered at address: cache.master");

        // Deploy the first slave
        deployInitialSlave();
    }

    private void deployInitialSlave() {
        String slaveAddress = "slave-initial";
        io.vertx.core.json.JsonObject config = new io.vertx.core.json.JsonObject().put("slaveAddress", slaveAddress);
        io.vertx.core.DeploymentOptions options = new io.vertx.core.DeploymentOptions().setConfig(config);

        vertx.deployVerticle("com.objstore.slave.SlaveVerticle", options, ar -> {
            if (ar.succeeded()) {
                System.out.println("Initial slave deployed with address: " + slaveAddress);
            } else {
                System.err.println("Failed to deploy initial slave: " + ar.cause());
            }
        });
    }

    private void handleCacheRequest(Message message) {
        CacheMessage request = (CacheMessage) message.body();
        System.out.println("Processing " + request.operation + " for key: " + request.key);

        if (slaves.isEmpty()) {
            System.out.println("No slaves available, failing request");
            message.fail(404, "No slaves available");
            return;
        }

        // Simple hash-based routing
        String targetSlave = selectSlave(request.key);
        System.out.println("Routing request to slave: " + targetSlave);

        vertx.eventBus().request(targetSlave, request, reply -> {
            if (reply.succeeded()) {
                message.reply(reply.result().body());
            } else {
                System.err.println("Slave request failed: " + reply.cause());
                message.fail(500, reply.cause().getMessage());
            }
        });
    }

    private String selectSlave(String key) {
        // Simple hash-based routing
        int index = Math.abs(key.hashCode() % slaves.size());
        return slaves.get(index);
    }
}
