package com.sample;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MyExternalServiceSinkConfig extends AbstractConfig {


    public static final String CONNECTION_URL = "connection.url";
    private static final String CONNECTION_URL_DOC = "Destination url";
    private static final String CONNECTION_URL_DISPLAY = "Destrination URL";

    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final int RETRY_BACKOFF_MS_DEFAULT = 3000;
    private static final String RETRY_BACKOFF_MS_DOC =
            "The time in milliseconds to wait following an error before a retry attempt is made.";
    private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";

    public static final String MAX_RETRIES = "max.retries";
    private static final int MAX_RETRIES_DEFAULT = 10;
    private static final String MAX_RETRIES_DOC =
            "The maximum number of times to retry on errors before failing the task.";
    private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";

    private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);

    private static final ConfigDef.Range MINUS_1_MINIMUM_INT_VALIDATOR = ConfigDef.Range.atLeast(-1);


    private static final String CONNECTION_GROUP = "Connection";
    private static final String RETRIES_GROUP = "Retries";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    CONNECTION_URL,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    CONNECTION_URL_DOC,
                    CONNECTION_GROUP,
                    1,
                    ConfigDef.Width.LONG,
                    CONNECTION_URL_DISPLAY)
            .define(
                    MAX_RETRIES,
                    ConfigDef.Type.INT,
                    MAX_RETRIES_DEFAULT,
                    MINUS_1_MINIMUM_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM,
                    MAX_RETRIES_DOC,
                    RETRIES_GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    MAX_RETRIES_DISPLAY
            )
            .define(
                    RETRY_BACKOFF_MS,
                    ConfigDef.Type.INT,
                    RETRY_BACKOFF_MS_DEFAULT,
                    NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM,
                    RETRY_BACKOFF_MS_DOC,
                    RETRIES_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    RETRY_BACKOFF_MS_DISPLAY
            );


    public final String url;
    public final Integer maxRetries;
    public final Integer retryBackoffMs;

    public MyExternalServiceSinkConfig(Map<?,?> props) {
        super(CONFIG_DEF,props);
        url = getString(CONNECTION_URL);
        maxRetries = getInt(MAX_RETRIES);
        retryBackoffMs = getInt(RETRY_BACKOFF_MS);
    }
}
