package com.objstore;

import com.objstore.master.MasterVerticle;
import com.objstore.slave.SlaveVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class MainApp {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        if (args.length > 0 && "SLAVE".equals(args[0])) {
            String address = args[1];
            DeploymentOptions options = new DeploymentOptions()
                    .setConfig(new JsonObject().put("slaveAddress", address));
            vertx.deployVerticle(new SlaveVerticle(), options);
        } else {
            vertx.deployVerticle(new MasterVerticle());
        }
    }
}