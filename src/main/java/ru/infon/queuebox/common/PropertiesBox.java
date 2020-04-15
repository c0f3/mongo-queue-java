package ru.infon.queuebox.common;

import java.util.Properties;

public class PropertiesBox extends Properties {

    public PropertiesBox(Properties properties) {
        putAll(properties);
    }

    public int tryGetIntProperty(String propertyName, int defaultValue) {
        try {
            Object value = get(propertyName);
            if (value == null) {
                return defaultValue;
            }
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException ignore) {
            return defaultValue;
        }
    }

}
