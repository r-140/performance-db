package com.iu.dbserver;

import com.iu.service.ThreadPoolInstance;
import com.iu.worker.TaskType;
import com.message.MessageBean;
import com.util.PropertyReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Created by Illia_Ushakov on 4/15/2019.
 */
// TODO: 4/22/2019 consider implementation via java.nio.channels.SelectorProvider which is epoll implementation see here
//    https://stackoverflow.com/questions/10621783/is-there-epoll-equivalent-in-java
public class PooledDbServer {
    private static final Logger LOGGER = Logger.getLogger(PooledDbServer.class.getName());

    //    todo move initialization of properties to static block
    private static final String DB_SERVER_PROPS_FILE = "/dbserver.properties";

    private static final String PORT = "port";
    private static final String CORE_POOL_SIZE = "corePoolSize";
    private static final String MAX_POOL_SIZE = "maxPoolSize";
    private static final String KEEP_ALIVE_TIME = "keepAliveTime";
    private static final String MAX_QUEUE_CAPACITY = "maxQueueCapacity";
    private static final String DISC_PATH = "discPath";

    public static void main(String[] args) {

        try {

            final int port = Integer.parseInt(getProperty(PORT));
            final int corePoolSize = Integer.parseInt(getProperty(CORE_POOL_SIZE));
            final int maxPoolSize = Integer.parseInt(getProperty(MAX_POOL_SIZE));
            final int keepAliveTime = Integer.parseInt(getProperty(KEEP_ALIVE_TIME));
            final int maxQueueCapacity = Integer.parseInt(getProperty(MAX_QUEUE_CAPACITY));
            final String discPath = getProperty(DISC_PATH);

//            TODO add validation of required parameters
//            TODO replace logger with log4j2 one
            LOGGER.log(Level.FINE, "port {}", port);

            ThreadPoolInstance.INSTANCE.init(corePoolSize, maxPoolSize, keepAliveTime, maxQueueCapacity, discPath);

            final ServerSocket server = new ServerSocket(port);

            while (true) {
                LOGGER.log(Level.INFO, "Waiting for the client request");
                final Socket connection = server.accept();

                final MessageBean messageBean = getTaskType(connection.getInputStream());

                LOGGER.log(Level.FINE, String.format("Message bean %s", messageBean));

                final TaskType taskType = TaskType.getTaskByType(messageBean.getTaskType());

                LOGGER.log(Level.INFO, String.format("task type %s", taskType));

                if (taskType != null) {
                    ThreadPoolInstance.INSTANCE.submitTask(taskType.getTask(connection, messageBean.getPayload()));
                } else {
                    LOGGER.log(Level.INFO, "not possible to find appropriate task");
                }
            }
        } catch (IOException ex) {
            LOGGER.log(Level.INFO, String.format("Couldn't start server %s", ex.getMessage()));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static String getProperty(final String propertyName) throws IOException {
        return PropertyReader.getProperty(DB_SERVER_PROPS_FILE, propertyName);
    }

    private static MessageBean getTaskType(InputStream inputStream) throws IOException, ClassNotFoundException {

        ObjectInputStream ois = new ObjectInputStream(inputStream);
        //convert ObjectInputStream object to String
        final MessageBean message = (MessageBean) ois.readObject();
        LOGGER.log(Level.FINER, String.format("getTaskType(): Message Received: %s", message));

        return message;
    }

}
