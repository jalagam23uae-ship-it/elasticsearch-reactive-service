package com.microservices.elasticsearch.dynamic.query.controller;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.microservices.elasticsearch.dynamic.query.dto.ElasticsearchQueryRequest;
import com.microservices.elasticsearch.dynamic.query.dto.ElasticsearchResponse;
import com.microservices.elasticsearch.dynamic.query.dto.SearchRequest;
import com.microservices.elasticsearch.dynamic.query.dto.SearchResult;
import com.microservices.elasticsearch.dynamic.query.service.ElasticsearchService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * REST Controller for Elasticsearch operations
 * Provides both reactive and virtual thread-based endpoints
 */
@Slf4j
@RestController
@RequestMapping("/api/elasticsearch")
@RequiredArgsConstructor
public class ElasticsearchController {

    private final ElasticsearchService elasticsearchService;

    @PostMapping("/search/{indexName}")
    public Mono<ResponseEntity<SearchResult<Map>>> searchReactive(
            @PathVariable String indexName,
            @Valid @RequestBody ElasticsearchQueryRequest request) {

        log.info("Reactive search request for index: {}", indexName);

        return elasticsearchService
                .searchAsync(indexName, request, Map.class)
                .map(ResponseEntity::ok)
                .onErrorReturn(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build())
                .doOnSuccess(result -> log.info("Reactive search completed for index: {}", indexName))
                .doOnError(error -> log.error("Reactive search failed for index: {}", indexName, error));
    }

    @PostMapping("/search-vt/{indexName}")
    public CompletableFuture<ResponseEntity<SearchResult<Map>>> searchWithVirtualThreads(
            @PathVariable String indexName,
            @Valid @RequestBody ElasticsearchQueryRequest request) {

        log.info("Virtual thread search request for index: {}", indexName);

        return elasticsearchService
                .searchWithVirtualThreads(indexName, request, Map.class)
                .thenApply(ResponseEntity::ok)
                .exceptionally(throwable -> {
                    log.error("Virtual thread search failed for index: {}", indexName, throwable);
                    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
                });
    }

    @GetMapping("/health")
    public Mono<ResponseEntity<Map<String, String>>> health() {
        return Mono.just(ResponseEntity.ok(Map.of(
                "status", "UP",
                "virtual_threads", "enabled",
                "reactive", "enabled",
                "elasticsearch_client", "direct",
                "version", "1.0.0")));
    }

    @GetMapping("/index/{indexName}/info")
    public Mono<ResponseEntity<Map<String, Object>>> getIndexInfo(@PathVariable String indexName) {
        log.info("Index info request for: {}", indexName);

        return Mono.just(ResponseEntity.ok(Map.of(
                "index", indexName,
                "status", "available",
                "operations", List.of("search", "aggregate", "stream", "batch"))));
    }

    @PostMapping("/batch-search")
    public Flux<SearchResult<Map>> batchSearch(
            @Valid @RequestBody List<SearchRequest<Map>> requests) {

        log.info("Batch search request for {} indices", requests.size());

        return elasticsearchService.batchSearch(requests)
                .doOnComplete(() -> log.info("Batch search completed"))
                .doOnError(error -> log.error("Batch search failed", error));
    }

    @PostMapping(value = "/search-stream/{indexName}", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<Map> searchStream(
            @PathVariable String indexName,
            @Valid @RequestBody ElasticsearchQueryRequest request) {

        log.info("Stream search request for index: {}", indexName);

        return elasticsearchService.searchStream(indexName, request, Map.class)
                .doOnComplete(() -> log.info("Stream search completed for index: {}", indexName))
                .doOnError(error -> log.error("Stream search failed for index: {}", indexName, error));
    }

    @PostMapping("/aggregation/{indexName}")
    public Mono<ResponseEntity<Map<String, Object>>> executeAggregation(
            @PathVariable String indexName,
            @Valid @RequestBody ElasticsearchQueryRequest request) {

        log.info("Aggregation request for index: {}", indexName);

        return elasticsearchService
                .executeAggregation(indexName, request)
                .map(ResponseEntity::ok)
                .onErrorReturn(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build())
                .doOnSuccess(result -> log.info("Aggregation completed for index: {}", indexName))
                .doOnError(error -> log.error("Aggregation failed for index: {}", indexName, error));
    }

    @PostMapping("/raw-query/{indexName}")
    public CompletableFuture<ResponseEntity<ElasticsearchResponse<Map<String, Object>>>> executeRawQuery(
            @PathVariable String indexName,
            @RequestBody Map<String, Object> esQuery) {

        log.info("Raw query request for index: {}", indexName);

        return elasticsearchService
                .executeRawQuery(indexName, esQuery)
                .thenApply(ResponseEntity::ok)
                .exceptionally(throwable -> {
                    log.error("Raw query failed for index: {}", indexName, throwable);
                    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
                });
    }

    /**
     * Reactive Spring Data operations-based search
     */
    @PostMapping(value = "/ops-search/{indexName}", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<Map> searchWithReactiveOperations(
            @PathVariable String indexName,
            @Valid @RequestBody ElasticsearchQueryRequest request) {
        log.info("ReactiveElasticsearchOperations search request for index: {}", indexName);
        return elasticsearchService.searchWithReactiveOps(indexName, request);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleException(Exception e) {
        log.error("Controller exception occurred", e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of(
                        "error", e.getMessage(),
                        "status", "error",
                        "timestamp", System.currentTimeMillis()));
    }
}
