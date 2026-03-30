package com.iu.dbserver;

import com.iu.service.VirtualThreadDbServer;
import com.util.PropertyReader;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Entry point — delegates to {@link VirtualThreadDbServer}.
 */
public class PooledDbServer {
    private static final Logger LOGGER = Logger.getLogger(PooledDbServer.class.getName());
    private static final String PROPS  = "/dbserver.properties";

    public static void main(String[] args) {
        try {
            int    port          = Integer.parseInt(PropertyReader.getProperty(PROPS, "port"));
            int    corePoolSize  = Integer.parseInt(PropertyReader.getProperty(PROPS, "corePoolSize"));
            int    maxPoolSize   = Integer.parseInt(PropertyReader.getProperty(PROPS, "maxPoolSize"));
            int    keepAlive     = Integer.parseInt(PropertyReader.getProperty(PROPS, "keepAliveTime"));
            int    maxQueueCap   = Integer.parseInt(PropertyReader.getProperty(PROPS, "maxQueueCapacity"));
            String discPath      = PropertyReader.getProperty(PROPS, "discPath");

            VirtualThreadDbServer server = new VirtualThreadDbServer(
                    port, corePoolSize, maxPoolSize, keepAlive, maxQueueCap, discPath);
            server.start();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to start server", e);
        }
    }
}
