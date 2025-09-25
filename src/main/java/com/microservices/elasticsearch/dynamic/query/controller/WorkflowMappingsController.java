package com.microservices.elasticsearch.dynamic.query.controller;

import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.microservices.elasticsearch.dynamic.query.service.WorkflowMappingsService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequestMapping("/api/workflow-mappings")
@RequiredArgsConstructor
public class WorkflowMappingsController {

    private final WorkflowMappingsService service;

    @GetMapping(produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<Map<String, Object>> getAll() {
        log.info("Fetching all workflow mappings");
        return service.getAll();
    }

    @GetMapping("/{mappingName}")
    public Mono<ResponseEntity<Map<String, Object>>> getByName(@PathVariable String mappingName) {
        log.info("Fetching workflow mapping: {}", mappingName);
        return service.getByMappingName(mappingName)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.status(HttpStatus.NOT_FOUND).build());
    }
}

