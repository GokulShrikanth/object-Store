package com.objstore.slave;

import com.objstore.common.CacheMessage;
import com.objstore.common.LRUCache;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;

import java.io.IOException;
import java.io.Serializable;

public class SlaveVerticle extends AbstractVerticle {
    private LRUCache<String, Serializable> cache;

    @Override
    public void start() {
        String address = config().getString("slaveAddress");
        cache = new LRUCache<>(10000);

        vertx.setPeriodic(5000, id -> checkMemoryUsage(address));

        vertx.eventBus().consumer(address, (Message<CacheMessage> msg) -> {
            CacheMessage cm = msg.body();
            if ("PUT".equals(cm.operation)) {
                cache.put(cm.key, cm.value);
                msg.reply("OK");
            } else if ("GET".equals(cm.operation)) {
                Serializable value = cache.get(cm.key);
                msg.reply(value);
            }
        });

        vertx.eventBus().send("slave.registration", address);
    }

    private void checkMemoryUsage(String myAddress) {
        Runtime rt = Runtime.getRuntime();
        long used = rt.totalMemory() - rt.freeMemory();

        if (used > 1.8 * 1024 * 1024 * 1024) {
            spawnNewSlave(myAddress);
        }
    }

    private void spawnNewSlave(String myAddress) {
        try {
            int newId = (int) (System.currentTimeMillis() % 100000);
            String newSlaveAddress = "slave-" + newId;

            ProcessBuilder pb = new ProcessBuilder(
                    "java",
                    "-cp", "target/lru-object-store-vertx-1.0-SNAPSHOT.jar",
                    "com.example.MainApp",
                    "SLAVE", newSlaveAddress
            );
            pb.inheritIO();
            pb.start();
            System.out.println("Spawned new slave: " + newSlaveAddress);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}