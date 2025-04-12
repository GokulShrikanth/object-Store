package com.objstore.master;

import com.objstore.common.CacheMessage;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MasterVerticle extends AbstractVerticle {
    private final Map<String, String> keyToSlave = new ConcurrentHashMap<>();
    private final List<String> slaveAddresses = new ArrayList<>();
    private int slaveCounter = 0;

    @Override
    public void start(Promise<Void> startPromise) {
        vertx.eventBus().consumer("slave.registration", msg -> {
            String addr = (String) msg.body();
            if (!slaveAddresses.contains(addr)) {
                slaveAddresses.add(addr);
                System.out.println("Registered new slave: " + addr);
            }
        });

        deployNewSlave();

        vertx.eventBus().consumer("cache.master", msg -> {
            CacheMessage cm = (CacheMessage) msg.body();
            if ("PUT".equals(cm.operation)) {
                String target = pickTargetSlave(cm.key);
                vertx.eventBus().request(target, cm, reply -> {
                    if (reply.succeeded()) msg.reply(reply.result().body());
                    else msg.fail(500, "Slave failed");
                });
            } else if ("GET".equals(cm.operation)) {
                String target = keyToSlave.get(cm.key);
                if (target != null) {
                    vertx.eventBus().request(target, cm, reply -> {
                        if (reply.succeeded()) msg.reply(reply.result().body());
                        else msg.fail(404, "Not found in slave");
                    });
                } else {
                    msg.fail(404, "Key not found");
                }
            }
        });

        startPromise.complete();
    }

    private String pickTargetSlave(String key) {
        String address = slaveAddresses.get(slaveCounter % slaveAddresses.size());
        keyToSlave.put(key, address);
        slaveCounter++;
        return address;
    }

    private void deployNewSlave() {
        String addr = "slave-" + slaveCounter++;
        DeploymentOptions options = new DeploymentOptions().setConfig(new JsonObject().put("slaveAddress", addr));
        vertx.deployVerticle("com.example.slave.SlaveVerticle", options);
        slaveAddresses.add(addr);
    }
}