package com.sample;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyExternalServiceConnector extends SinkConnector {
    private final Logger log = LoggerFactory.getLogger(MyExternalServiceConnector.class);

    private Map<String, String> configProps;


    @Override
    public Class<? extends Task> taskClass() {
        return MyExternalServiceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Setting task configurations for {} tasks.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void start(Map<String, String> props) {
        configProps = props;
    }


    @Override
    public void stop() {
        //nothing particular to do for this use case
    }

    @Override
    public ConfigDef config() {
        return MyExternalServiceSinkConfig.CONFIG_DEF;
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        return super.validate(connectorConfigs);
    }


    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
