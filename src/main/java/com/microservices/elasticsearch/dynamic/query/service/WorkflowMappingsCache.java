package com.microservices.elasticsearch.dynamic.query.service;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class WorkflowMappingsCache {

    private final ConcurrentHashMap<String, Map<String, Object>> cache = new ConcurrentHashMap<>();

    public void put(String mappingName, Map<String, Object> row) {
        if (mappingName == null || mappingName.isBlank() || row == null) return;
        cache.put(mappingName, row);
    }

    public Map<String, Object> get(String mappingName) {
        return cache.get(mappingName);
    }

    public Map<String, Map<String, Object>> snapshot() {
        return Collections.unmodifiableMap(cache);
    }

    public int size() {
        return cache.size();
    }

    public void clear() {
        cache.clear();
    }
}

