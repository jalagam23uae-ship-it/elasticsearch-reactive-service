package com.microservices.elasticsearch.dynamic.query.service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
@Service
public class WorkflowMappingsService {

    private static final String RESOURCE_DB = "workflow_mappings.db";
    private Path dbPath;
    private String jdbcUrl;

    @PostConstruct
    public void init() {
        try {
            // Ensure JDBC driver is available
            try { Class.forName("org.sqlite.JDBC"); } catch (ClassNotFoundException ignore) { 
            	/* auto-register */ }

            // Copy the DB from classpath to a temp location that SQLite can open
            ClassPathResource resource = new ClassPathResource(RESOURCE_DB);
            if (!resource.exists()) {
                log.warn("Resource {} not found on classpath; workflow mappings will be unavailable", RESOURCE_DB);
                return;
            }
            this.dbPath = Files.createTempFile("workflow_mappings", ".db");
            try (InputStream in = resource.getInputStream()) {
                Files.copy(in, dbPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
            this.jdbcUrl = "jdbc:sqlite:" + dbPath.toAbsolutePath();
            log.info("Workflow mappings DB initialized at {}", this.jdbcUrl);
        } catch (IOException e) {
            log.error("Failed to initialize workflow mappings DB", e);
        }
    }

    public Flux<Map<String, Object>> getAll() {
        return Mono.fromCallable(() -> queryAll())
                .flatMapMany(Flux::fromIterable)
                .subscribeOn(Schedulers.boundedElastic());
    }

    public Mono<Map<String, Object>> getByMappingName(String mappingName) {
        return Mono.fromCallable(() -> queryByMappingName(mappingName))
                .flatMap(result -> result == null ? Mono.empty() : Mono.just(result))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private List<Map<String, Object>> queryAll() throws SQLException {
        if (jdbcUrl == null) return List.of();
        String sql = "SELECT * FROM workflow_mappings";
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            List<Map<String, Object>> list = new ArrayList<>();
            while (rs.next()) {
                list.add(mapRow(rs));
            }
            return list;
        }
    }

    private Map<String, Object> queryByMappingName(String mappingName) throws SQLException {
        if (jdbcUrl == null) return null;
        String sql = "SELECT * FROM workflow_mappings WHERE mapping_name = ? LIMIT 1";
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, mappingName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return mapRow(rs);
                }
                return null;
            }
        }
    }

    private Map<String, Object> mapRow(ResultSet rs) throws SQLException {
        Map<String, Object> row = new HashMap<>();
        ResultSetMetaData md = rs.getMetaData();
        int cols = md.getColumnCount();
        for (int i = 1; i <= cols; i++) {
            String name = md.getColumnLabel(i);
            Object val = rs.getObject(i);
            row.put(name, val);
        }
        return row;
    }
}

