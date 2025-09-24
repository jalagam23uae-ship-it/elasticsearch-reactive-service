package com.microservices.elasticsearch.dynamic.query.config;



import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.concurrent.Executor;
// Reactive beans are provided by Spring Boot auto-configuration when
// spring-boot-starter-data-elasticsearch is on the classpath.

/**
 * Configuration class for Elasticsearch client and virtual threads
 */
@Slf4j
@Configuration
@EnableAsync
public class ElasticsearchConfig {

    @Value("${app.elasticsearch.host:192.168.1.27}")
    private String elasticsearchHost;

    @Value("${app.elasticsearch.port:9200}")
    private int elasticsearchPort;

    @Value("${app.elasticsearch.scheme:http}")
    private String elasticsearchScheme;

    @Value("${app.elasticsearch.username:}")
    private String username;

    @Value("${app.elasticsearch.password:}")
    private String password;

    /**
     * Create virtual thread executor for async operations
     */
    @Bean(name = "virtualThreadExecutor")
    public Executor virtualThreadExecutor() {
        log.info("Creating virtual thread executor for async operations");
        return java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Create ElasticsearchClient bean for direct client operations
     */
    @Bean
    public ElasticsearchClient elasticsearchClient() {
        log.info("Configuring Elasticsearch client for {}://{}:{}", 
                elasticsearchScheme, elasticsearchHost, elasticsearchPort);
        
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(elasticsearchHost, elasticsearchPort, elasticsearchScheme)
        );

        // Add authentication if credentials are provided
        if (!username.isEmpty() && !password.isEmpty()) {
            log.info("Configuring Elasticsearch with authentication for user: {}", username);
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password)
            );
            
            builder.setHttpClientConfigCallback(httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
            );
        } else {
            log.info("Configuring Elasticsearch without authentication");
        }

        // Configure timeouts and connection settings
        builder.setRequestConfigCallback(requestConfigBuilder ->
                requestConfigBuilder
                        .setConnectTimeout(5000)
                        .setSocketTimeout(30000)
        );

        RestClient restClient = builder.build();
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        
        ElasticsearchClient client = new ElasticsearchClient(transport);
        log.info("Elasticsearch client configured successfully");
        
        return client;
    }

    // See Spring Boot's ReactiveElasticsearchClientAutoConfiguration.
}
