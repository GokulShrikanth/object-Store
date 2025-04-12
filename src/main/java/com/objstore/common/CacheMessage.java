package com.objstore.common;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;
import java.io.Serializable;

public class CacheMessage implements Serializable {
    public String operation;
    public String key;
    public Serializable value;

    // No-arg constructor required for serialization
    public CacheMessage() {}

    // Register codec with Vertx
    public static void registerCodec(io.vertx.core.Vertx vertx) {
        vertx.eventBus().registerDefaultCodec(
            CacheMessage.class,
            new CacheMessageCodec()
        );
    }

    // Inner codec class for CacheMessage
    static class CacheMessageCodec implements MessageCodec<CacheMessage, CacheMessage> {
        @Override
        public void encodeToWire(Buffer buffer, CacheMessage message) {
            JsonObject json = new JsonObject();
            json.put("operation", message.operation);
            json.put("key", message.key);
            if (message.value != null) {
                if (message.value instanceof String) {
                    json.put("value", (String)message.value);
                } else {
                    // For other serializable objects, you'd need proper serialization
                    json.put("value", message.value.toString());
                }
            }

            String jsonString = json.encode();
            buffer.appendInt(jsonString.length());
            buffer.appendString(jsonString);
        }

        @Override
        public CacheMessage decodeFromWire(int pos, Buffer buffer) {
            int length = buffer.getInt(pos);
            String jsonStr = buffer.getString(pos + 4, pos + 4 + length);
            JsonObject json = new JsonObject(jsonStr);

            CacheMessage message = new CacheMessage();
            message.operation = json.getString("operation");
            message.key = json.getString("key");
            message.value = json.getString("value");

            return message;
        }

        @Override
        public CacheMessage transform(CacheMessage message) {
            return message;
        }

        @Override
        public String name() {
            return "cacheMessage";
        }

        @Override
        public byte systemCodecID() {
            return -1;
        }
    }
}
