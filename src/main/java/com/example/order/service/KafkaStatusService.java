package com.example.order.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class KafkaStatusService {

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    public Map<String, Object> checkStatus() throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000");
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "3000");

        try (AdminClient adminClient = AdminClient.create(properties)) {
            Set<String> topics = adminClient.listTopics().names().get(3, TimeUnit.SECONDS);

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("bootstrapServers", bootstrapServers);
            result.put("connected", true);
            result.put("topics", topics);
            return result;
        }
    }
}
