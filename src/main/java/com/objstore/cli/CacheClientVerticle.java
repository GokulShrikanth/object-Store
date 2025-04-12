package com.objstore.cli;

import com.objstore.common.CacheMessage;
import io.vertx.core.AbstractVerticle;

public class CacheClientVerticle extends AbstractVerticle {

    @Override
    public void start() {
        String cmd = System.getProperty("cmd");
        String key = System.getProperty("key");
        String valueStr = System.getProperty("value", "");

        System.out.println("CacheClient starting with command: " + cmd + ", key: " + key);

        // Handle file reference format @/path/to/file
        if (valueStr.startsWith("@")) {
            try {
                String filePath = valueStr.substring(1);
                System.out.println("Reading value from file: " + filePath);
                valueStr = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(filePath)));
                System.out.println("Read " + valueStr.length() + " bytes from file");
            } catch (Exception e) {
                System.err.println("Failed to read value from file: " + e.getMessage());
                vertx.close();
                return;
            }
        }

        CacheMessage message = new CacheMessage();
        message.key = key;

        if ("put".equalsIgnoreCase(cmd)) {
            message.operation = "PUT";
            message.value = valueStr;

            System.out.println("Sending PUT request to cache.master for key: " + key);
            vertx.eventBus().request("cache.master", message, reply -> {
                if (reply.succeeded()) {
                    System.out.println("PUT success: " + reply.result().body());
                } else {
                    System.err.println("PUT failed: " + reply.cause().getMessage());
                }
                vertx.close();
            });
        } else if ("get".equalsIgnoreCase(cmd)) {
            message.operation = "GET";

            System.out.println("Sending GET request to cache.master for key: " + key);
            vertx.eventBus().request("cache.master", message, reply -> {
                if (reply.succeeded()) {
                    System.out.println("GET result: " + reply.result().body());
                } else {
                    System.err.println("GET failed: " + reply.cause().getMessage());
                }
                vertx.close();
            });
        } else {
            System.err.println("Unknown command: " + cmd);
            vertx.close();
        }
    }
}
