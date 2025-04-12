package com.objstore.common;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import java.io.Serializable;

public class CacheMessage implements Serializable {

    public String operation;
    public String key;
    public Serializable value;

    public CacheMessage() {
        // Default constructor required for serialization
    }

    public static void registerCodec(io.vertx.core.Vertx vertx) {
        // Register codec only once
        try {
            vertx.eventBus().registerDefaultCodec(
                CacheMessage.class,
                new CacheMessageCodec()
            );
            System.out.println("Registered CacheMessage codec");
        } catch (IllegalStateException e) {
            // Codec might already be registered
            System.out.println("CacheMessage codec already registered");
        }
    }

    // Custom codec to properly serialize/deserialize CacheMessage objects over the event bus
    public static class CacheMessageCodec implements MessageCodec<CacheMessage, CacheMessage> {

        @Override
        public void encodeToWire(Buffer buffer, CacheMessage message) {
            // Write operation
            Buffer opBuffer = Buffer.buffer(message.operation);
            buffer.appendInt(opBuffer.length());
            buffer.appendBuffer(opBuffer);

            // Write key
            Buffer keyBuffer = Buffer.buffer(message.key);
            buffer.appendInt(keyBuffer.length());
            buffer.appendBuffer(keyBuffer);

            // Write value (if present)
            if (message.value != null) {
                String valueStr = message.value.toString();
                Buffer valueBuffer = Buffer.buffer(valueStr);
                buffer.appendInt(valueBuffer.length());
                buffer.appendBuffer(valueBuffer);
            } else {
                buffer.appendInt(0);
            }
        }

        @Override
        public CacheMessage decodeFromWire(int pos, Buffer buffer) {
            CacheMessage message = new CacheMessage();

            // Read operation
            int opLength = buffer.getInt(pos);
            pos += 4;
            message.operation = buffer.getString(pos, pos + opLength);
            pos += opLength;

            // Read key
            int keyLength = buffer.getInt(pos);
            pos += 4;
            message.key = buffer.getString(pos, pos + keyLength);
            pos += keyLength;

            // Read value
            int valueLength = buffer.getInt(pos);
            pos += 4;
            if (valueLength > 0) {
                message.value = buffer.getString(pos, pos + valueLength);
            }

            return message;
        }

        @Override
        public CacheMessage transform(CacheMessage message) {
            // No transformation needed
            return message;
        }

        @Override
        public String name() {
            return "cachemessage";
        }

        @Override
        public byte systemCodecID() {
            return -1; // Always -1 for user-defined codecs
        }
    }
}
