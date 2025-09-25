package com.microservices.elasticsearch.dynamic.query.config;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.microservices.elasticsearch.dynamic.query.service.WorkflowMappingsCache;
import com.microservices.elasticsearch.dynamic.query.service.WorkflowMappingsService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
@Component
@RequiredArgsConstructor
public class WorkflowMappingsStartupLoader implements ApplicationRunner {

    private final WorkflowMappingsService service;
    private final WorkflowMappingsCache cache;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        try {
            log.info("Loading workflow mappings into cache at startup...");
            List<Map<String, Object>> rows = service.getAll()
                    .onErrorResume(e -> {
                        log.warn("Failed to read workflow mappings DB: {}", e.getMessage());
                        return Flux.empty();
                    })
                    .collectList()
                    .block(Duration.ofSeconds(15));

            if (rows == null) {
                log.info("No workflow mappings loaded (no rows).");
                return;
            }

            int added = 0;
            for (Map<String, Object> row : rows) {
                if (row == null || row.isEmpty()) continue;
                String key = extractMappingName(row);
                if (key != null) {
                    cache.put(key, row);
                    added++;
                }
            }

            log.info("Workflow mappings cache initialized with {} entries", added);
        } catch (Exception e) {
            log.warn("Workflow mappings cache initialization failed: {}", e.getMessage());
        }
    }

    private String extractMappingName(Map<String, Object> row) {
        // Case-insensitive lookup for column 'mapping_name'
        for (String k : row.keySet()) {
            if ("mapping_name".equalsIgnoreCase(k)) {
                Object v = row.get(k);
                return v != null ? String.valueOf(v) : null;
            }
        }
        return null;
    }
}

