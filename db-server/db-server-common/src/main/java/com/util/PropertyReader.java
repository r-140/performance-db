package com.util;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PropertyReader {
    private static final Logger LOGGER = Logger.getLogger(PropertyReader.class.getName());

    public static String getProperty(final String propertyFileName, final String propertyName) throws IOException {
        Properties prop = new Properties();
        InputStream input = PropertyReader.class.getResourceAsStream(propertyFileName);
        if (input == null) {
            LOGGER.log(Level.INFO, "unable to find file: {}", propertyName);
            throw new FileNotFoundException("unable to find file " + propertyFileName);
        }
        prop.load(input);

        return prop.getProperty(propertyName);

    }
}
