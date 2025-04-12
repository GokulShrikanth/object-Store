package com.objstore.cli;

import com.objstore.common.CacheMessage;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;

public class CacheClientVerticle extends AbstractVerticle {
    @Override
    public void start(Promise<Void> startPromise) {
        String cmd = System.getProperty("cmd");
        String key = System.getProperty("key");
        String value = System.getProperty("value");

        CacheMessage message = new CacheMessage();
        message.operation = cmd.toUpperCase();
        message.key = key;
        message.value = value;
        message.replyAddress = "cli.reply";

        vertx.eventBus().request("cache.master", message, reply -> {
            if (reply.succeeded()) {
                System.out.println("Response: " + reply.result().body());
            } else {
                System.out.println("Error: " + reply.cause().getMessage());
            }
            vertx.close();
        });

        startPromise.complete();
    }
}