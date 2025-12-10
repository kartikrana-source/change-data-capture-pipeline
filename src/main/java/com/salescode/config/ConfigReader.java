package com.salescode.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.salescode.models.RootConfig;

import java.io.InputStream;

public class ConfigReader {
    private static ConfigReader instance;
    private static RootConfig config;

    private static final String DEFAULT_CONFIG_FILE = "entity-config.yaml";

    /**
     * Private constructor - loads config on initialization
     */
    private ConfigReader() {
        this.config = loadConfig(DEFAULT_CONFIG_FILE);
    }

    /**
     * Get singleton instance
     */
    public static synchronized ConfigReader getInstance() {
        if (instance == null) {
            instance = new ConfigReader();
        }
        return instance;
    }


    /**
     * Load configuration from YAML file in resources folder
     */
    public RootConfig loadConfig(String configFile) {
        try {
            System.out.println("📖 Loading configuration from: " + configFile);
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(configFile);
            if (inputStream == null) {
                throw new RuntimeException("Config file not found in resources: " + configFile);
            }
            RootConfig config = mapper.readValue(inputStream, RootConfig.class);
            System.out.println("✅ Configuration loaded successfully");
            System.out.println("   Total entities: " + config.getEntities().size());
            config.getEntities().forEach(entity ->
                    System.out.println("   - " + entity.getEntityName() +
                            " (" + (entity.isEnabled() ? "enabled" : "disabled") + ")")
            );
            return config;
        } catch (Exception e) {
            System.err.println("❌ Failed to load configuration from " + configFile);
            e.printStackTrace();
            throw new RuntimeException("Configuration loading failed", e);
        }
    }
    public RootConfig loadConfig() {
        try {

            System.out.println("📖 Loading configuration from: " + DEFAULT_CONFIG_FILE);
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(DEFAULT_CONFIG_FILE);
            if (inputStream == null) {
                throw new RuntimeException("Config file not found in resources: " + DEFAULT_CONFIG_FILE);
            }
            RootConfig config = mapper.readValue(inputStream, RootConfig.class);
            System.out.println("✅ Configuration loaded successfully");
            System.out.println("   Total entities: " + config.getEntities().size());
            config.getEntities().forEach(entity ->
                    System.out.println("   - " + entity.getEntityName() +
                            " (" + (entity.isEnabled() ? "enabled" : "disabled") + ")")
            );
            return config;
        } catch (Exception e) {
            System.err.println("❌ Failed to load configuration from " + DEFAULT_CONFIG_FILE);
            e.printStackTrace();
            throw new RuntimeException("Configuration loading failed", e);
        }
    }

}
