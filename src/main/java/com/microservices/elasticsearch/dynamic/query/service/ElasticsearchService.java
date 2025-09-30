package com.microservices.elasticsearch.dynamic.query.service;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microservices.elasticsearch.dynamic.query.dto.ElasticsearchQueryRequest;
import com.microservices.elasticsearch.dynamic.query.dto.ElasticsearchResponse;
import com.microservices.elasticsearch.dynamic.query.dto.HitEnvelope;
import com.microservices.elasticsearch.dynamic.query.dto.SearchResult;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.WildcardQuery;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
@Service
@RequiredArgsConstructor
public class ElasticsearchService {

    private  final ElasticsearchClient elasticsearchClient;
    private final ElasticsearchQueryBuilderService queryBuilderService;
    private final ObjectMapper objectMapper=new ObjectMapper();
    private final ReactiveElasticsearchOperations reactiveElasticsearchOperations;

    public <T> Mono<SearchResult<T>> searchAsync(String indexName,
                                                 ElasticsearchQueryRequest queryRequest,
                                                 Class<T> targetClass) {
        return Mono.fromCallable(() -> {
            log.info("Building Elasticsearch query for index: {}", indexName);
            Map<String, Object> esQuery = queryBuilderService.buildEsQuery(queryRequest);
            log.info("esQuery: {}", esQuery);
            System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(esQuery));
            return executeSearch(indexName, esQuery, targetClass);
        })
        .subscribeOn(Schedulers.boundedElastic())
        .doOnSuccess(result -> log.info("Search completed for index: {} with {} results", indexName, result.getTotalHits()))
        .doOnError(error -> log.error("Search failed for index: {}", indexName, error));
    }

    @Async("virtualThreadExecutor")
    public <T> CompletableFuture<SearchResult<T>> searchWithVirtualThreads(String indexName,
                                                                           ElasticsearchQueryRequest queryRequest,
                                                                           Class<T> targetClass) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("Executing search with virtual thread for index: {}", indexName);
                Map<String, Object> esQuery = queryBuilderService.buildEsQuery(queryRequest);
				/*
				 * log.info("Executing search with virtual thread for esQuery : {}", esQuery);
				 */
                System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(esQuery));
                return executeSearch(indexName, esQuery, targetClass);
            } catch (Exception e) {
                log.error("Virtual thread search failed for index: {}", indexName, e);
                throw new RuntimeException("Search failed", e);
            }
        }, java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
    }

    public Flux<SearchResult<Map>> batchSearch(
            List<com.microservices.elasticsearch.dynamic.query.dto.SearchRequest<Map>> searchRequests) {
        return Flux.fromIterable(searchRequests)
                .flatMap(request -> searchAsync(request.getIndexName(), request.getQueryRequest(), Map.class)
                        .onErrorResume(error -> {
                            log.error("Batch search failed for index: {}", request.getIndexName(), error);
                            return Mono.just(SearchResult.<Map>builder()
                                    .documents(List.of())
                                    .totalHits(0L)
                                    .build());
                        }))
                .doOnComplete(() -> log.info("Batch search completed"));
    }

    public <T> Flux<T> searchStream(String indexName,
                                    ElasticsearchQueryRequest queryRequest,
                                    Class<T> targetClass) {
        return Mono.fromCallable(() -> queryBuilderService.buildEsQuery(queryRequest))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMapMany(esQuery -> {
                    try {
                        SearchResult<T> result = executeSearch(indexName, esQuery, targetClass);
                        return Flux.fromIterable(result.getDocuments());
                    } catch (Exception e) {
                        return Flux.error(e);
                    }
                })
                .doOnComplete(() -> log.info("Stream search completed for index: {}", indexName));
    }

    /**
     * Reactive search using Spring Data ReactiveElasticsearchOperations.
     */
    public Flux<Map> searchWithReactiveOps(String indexName,
                                           ElasticsearchQueryRequest queryRequest) {
        return Mono.fromCallable(() -> queryBuilderService.buildEsQuery(queryRequest))
                .map(this::convertToElasticsearchQuery)
                .flatMapMany(query -> {
                    NativeQuery nativeQuery = NativeQuery.builder().withQuery(query).build();
                    return reactiveElasticsearchOperations
                            .search(nativeQuery, Map.class, IndexCoordinates.of(indexName))
                            .map(SearchHit::getContent);
                })
                .doOnSubscribe(s -> log.info("ReactiveOps search started for index: {}", indexName))
                .doOnComplete(() -> log.info("ReactiveOps search completed for index: {}", indexName));
    }

    public Mono<Map<String, Object>> executeAggregation(String indexName,
                                                        ElasticsearchQueryRequest queryRequest) {
        return Mono.fromCallable(() -> {
            Map<String, Object> esQuery = queryBuilderService.buildEsQuery(queryRequest);
            try {
                SearchRequest.Builder searchBuilder = new SearchRequest.Builder()
                        .index(indexName)
                        .size(0)
                        .trackTotalHits(t -> t.enabled(true));

                Query query = convertToElasticsearchQuery(esQuery);
                searchBuilder.query(query);

                SearchRequest searchRequest = searchBuilder.build();
                SearchResponse<Map> response = elasticsearchClient.search(searchRequest, Map.class);

                return Map.<String, Object>of(
                        "aggregations", response.aggregations(),
                        "total_hits", response.hits().total() != null ? response.hits().total().value() : 0L);
            } catch (Exception e) {
                throw new RuntimeException("Aggregation failed", e);
            }
        })
        .subscribeOn(Schedulers.boundedElastic())
        .doOnSuccess(aggs -> log.info("Aggregation completed for index: {}", indexName));
    }

    @Async("virtualThreadExecutor")
    public CompletableFuture<ElasticsearchResponse<Map<String, Object>>> executeRawQuery(
            String indexName, Map<String, Object> esQuery) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                SearchRequest.Builder searchBuilder = new SearchRequest.Builder()
                        .index(indexName)
                        .trackTotalHits(t -> t.enabled(true));

                Query query = convertToElasticsearchQuery(esQuery);
                searchBuilder.query(query);

                if (esQuery.containsKey("from")) {
                    searchBuilder.from((Integer) esQuery.get("from"));
                }
                if (esQuery.containsKey("size")) {
                    searchBuilder.size((Integer) esQuery.get("size"));
                }

                // Apply sort options if present
                applySorts(searchBuilder, esQuery);

                SearchRequest searchRequest = searchBuilder.build();
                SearchResponse<Map> response = elasticsearchClient.search(searchRequest, Map.class);
                return convertToElasticsearchResponse(response);
            } catch (Exception e) {
                log.error("Raw query execution failed for index: {}", indexName, e);
                throw new RuntimeException("Raw query failed", e);
            }
        }, java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
    }

    private <T> SearchResult<T> executeSearch(String indexName,
                                              Map<String, Object> esQuery,
                                              Class<T> targetClass) {
        try {
            SearchRequest.Builder searchBuilder = new SearchRequest.Builder()
                    .index(indexName)
                    .trackTotalHits(t -> t.enabled(true));

            Query query = convertToElasticsearchQuery(esQuery);
            searchBuilder.query(query);

            if (esQuery.containsKey("from")) {
                searchBuilder.from((Integer) esQuery.get("from"));
            }
            if (esQuery.containsKey("size")) {
                searchBuilder.size((Integer) esQuery.get("size"));
            }

            if (esQuery.containsKey("_source")) {
                Object sourceCfg = esQuery.get("_source");
                try {
                    if (sourceCfg instanceof List<?> list) {
                        List<String> includes = list.stream().map(String::valueOf).toList();
                        searchBuilder.source(s -> s.filter(f -> f.includes(includes)));
                    } else if (sourceCfg instanceof Map<?, ?> m) {
                        Object inc = m.get("includes");
                        Object exc = m.get("excludes");
                        List<String> includes = inc instanceof List<?> li ? li.stream().map(String::valueOf).toList() : List.of();
                        List<String> excludes = exc instanceof List<?> le ? le.stream().map(String::valueOf).toList() : List.of();
                        searchBuilder.source(s -> s.filter(f -> f.includes(includes).excludes(excludes)));
                    } else if (sourceCfg instanceof Boolean b) {
                        boolean fetch = (Boolean) b;
                        searchBuilder.source(s -> s.fetch(fetch));
                    } else if (sourceCfg instanceof String s) {
                        // Single field convenience
                        searchBuilder.source(sc -> sc.filter(f -> f.includes(s)));
                    }
                } catch (Exception ignore) {
                    // Fallback: ignore malformed _source config
                }
            }

            // Apply sort options if present
            applySorts(searchBuilder, esQuery);

            SearchRequest searchRequest = searchBuilder.build();
            SearchResponse<Map> resp = elasticsearchClient.search(searchRequest, Map.class);
            log.info("total={} took={} hits={}",
                     resp.hits().total() == null ? null : resp.hits().total().value(),
                     resp.took(),
                     resp.hits().hits().size());

            log.info("response:::::::::",resp);
            return convertElasticsearchResponse(resp, targetClass, esQuery);
        } catch (Exception e) {
            log.error("Search execution failed for index: {}", indexName, e);
            throw new RuntimeException("Search failed", e);
        }
    }

    private void applySorts(SearchRequest.Builder searchBuilder, Map<String, Object> esQuery) {
        if (!esQuery.containsKey("sort")) return;
        try {
            List<Map<String, Object>> sortList = (List<Map<String, Object>>) esQuery.get("sort");
            for (Map<String, Object> sortSpec : sortList) {
                if (sortSpec == null || sortSpec.isEmpty()) continue;
                String field = sortSpec.keySet().iterator().next();
                Object spec = sortSpec.get(field);
                String order = "asc";
                if (spec instanceof Map<?, ?> m) {
                    Object o = m.get("order");
                    if (o instanceof String s) order = s;
                }
                final String ord = order;
                searchBuilder.sort(s -> s.field(f -> f.field(field)
                        .order("desc".equalsIgnoreCase(ord) ? SortOrder.Desc : SortOrder.Asc)));
            }
        } catch (Exception e) {
            log.warn("Failed to apply sort options: {}", e.getMessage());
        }
    }

    private Query convertToElasticsearchQuery(Map<String, Object> esQuery) {
        if (!esQuery.containsKey("query")) {
            return Query.of(q -> q.matchAll(m -> m));
        }
        Map<String, Object> queryMap = (Map<String, Object>) esQuery.get("query");
        return convertQueryMap(queryMap);
    }

    private Query convertQueryMap(Map<String, Object> queryMap) {
        if (queryMap.containsKey("bool")) {
            return convertBoolQuery((Map<String, Object>) queryMap.get("bool"));
        }
        if (queryMap.containsKey("term")) {
            return convertTermQuery((Map<String, Object>) queryMap.get("term"));
        }
        if (queryMap.containsKey("range")) {
            return convertRangeQuery((Map<String, Object>) queryMap.get("range"));
        }
        if (queryMap.containsKey("match")) {
            return convertMatchQuery((Map<String, Object>) queryMap.get("match"));
        }
        if (queryMap.containsKey("wildcard")) {
            return convertWildcardQuery((Map<String, Object>) queryMap.get("wildcard"));
        }
        if (queryMap.containsKey("terms")) {
            return convertTermsQuery((Map<String, Object>) queryMap.get("terms"));
        }
        if (queryMap.containsKey("exists")) {
            return convertExistsQuery((Map<String, Object>) queryMap.get("exists"));
        }
        return Query.of(q -> q.matchAll(m -> m));
    }

    private Query convertBoolQuery(Map<String, Object> boolMap) {
        BoolQuery.Builder boolBuilder = new BoolQuery.Builder();

        if (boolMap.containsKey("must")) {
            List<Map<String, Object>> mustClauses = (List<Map<String, Object>>) boolMap.get("must");
            for (Map<String, Object> clause : mustClauses) {
                boolBuilder.must(convertQueryMap(clause));
            }
        }
        if (boolMap.containsKey("should")) {
            List<Map<String, Object>> shouldClauses = (List<Map<String, Object>>) boolMap.get("should");
            for (Map<String, Object> clause : shouldClauses) {
                boolBuilder.should(convertQueryMap(clause));
            }
        }
        if (boolMap.containsKey("must_not")) {
            List<Map<String, Object>> mustNotClauses = (List<Map<String, Object>>) boolMap.get("must_not");
            for (Map<String, Object> clause : mustNotClauses) {
                boolBuilder.mustNot(convertQueryMap(clause));
            }
        }
        if (boolMap.containsKey("minimum_should_match")) {
            boolBuilder.minimumShouldMatch(String.valueOf(boolMap.get("minimum_should_match")));
        }
        return Query.of(q -> q.bool(boolBuilder.build()));
    }

    private Query convertTermQuery(Map<String, Object> termMap) {
        String field = termMap.keySet().iterator().next();
        Object valueObj = termMap.get(field);
        if (valueObj instanceof Map) {
            Map<String, Object> termValue = (Map<String, Object>) valueObj;
            Object value = termValue.get("value");
            TermQuery.Builder termBuilder = new TermQuery.Builder().field(field).value(toFieldValue(value));
            if (termValue.containsKey("boost")) {
                termBuilder.boost(((Number) termValue.get("boost")).floatValue());
            }
            return Query.of(q -> q.term(termBuilder.build()));
        } else {
            return Query.of(q -> q.term(t -> t.field(field).value(toFieldValue(valueObj))));
        }
    }

    private Query convertRangeQuery(Map<String, Object> rangeMap) {
        String field = rangeMap.keySet().iterator().next();
        Map<String, Object> rangeValue = (Map<String, Object>) rangeMap.get(field);
        RangeQuery.Builder rangeBuilder = new RangeQuery.Builder().field(field);
        if (rangeValue.containsKey("gte")) {
            rangeBuilder.gte(JsonData.of(rangeValue.get("gte")));
        }
        if (rangeValue.containsKey("lte")) {
            rangeBuilder.lte(JsonData.of(rangeValue.get("lte")));
        }
        if (rangeValue.containsKey("gt")) {
            rangeBuilder.gt(JsonData.of(rangeValue.get("gt")));
        }
        if (rangeValue.containsKey("lt")) {
            rangeBuilder.lt(JsonData.of(rangeValue.get("lt")));
        }
        return Query.of(q -> q.range(rangeBuilder.build()));
    }

    private Query convertMatchQuery(Map<String, Object> matchMap) {
        String field = matchMap.keySet().iterator().next();
        Object valueObj = matchMap.get(field);
        if (valueObj instanceof Map) {
            Map<String, Object> matchValue = (Map<String, Object>) valueObj;
            Object query = matchValue.get("query");
            MatchQuery.Builder matchBuilder = new MatchQuery.Builder().field(field).query(toFieldValue(query));
            if (matchValue.containsKey("boost")) {
                matchBuilder.boost(((Number) matchValue.get("boost")).floatValue());
            }
            return Query.of(q -> q.match(matchBuilder.build()));
        } else {
            return Query.of(q -> q.match(m -> m.field(field).query(toFieldValue(valueObj))));
        }
    }

    private Query convertWildcardQuery(Map<String, Object> wildcardMap) {
        String field = wildcardMap.keySet().iterator().next();
        Object valueObj = wildcardMap.get(field);
        if (valueObj instanceof Map) {
            Map<String, Object> wildcardValue = (Map<String, Object>) valueObj;
            String value = (String) wildcardValue.get("value");
            WildcardQuery.Builder wildcardBuilder = new WildcardQuery.Builder().field(field).value(value);
            if (wildcardValue.containsKey("boost")) {
                wildcardBuilder.boost(((Number) wildcardValue.get("boost")).floatValue());
            }
            return Query.of(q -> q.wildcard(wildcardBuilder.build()));
        } else {
            return Query.of(q -> q.wildcard(w -> w.field(field).value((String) valueObj)));
        }
    }

    private Query convertTermsQuery(Map<String, Object> termsMap) {
        String field = termsMap.keySet().iterator().next();
        List<Object> values = (List<Object>) termsMap.get(field);
        List<FieldValue> fieldValues = values.stream().map(this::toFieldValue).collect(Collectors.toList());
        return Query.of(q -> q.terms(t -> t.field(field).terms(terms -> terms.value(fieldValues))));
    }

    private Query convertExistsQuery(Map<String, Object> existsMap) {
        String field = (String) existsMap.get("field");
        return Query.of(q -> q.exists(e -> e.field(field)));
    }

    private <T> SearchResult<T> convertElasticsearchResponse(SearchResponse<Map> response,
                                                             Class<T> targetClass,
                                                             Map<String, Object> esQuery) {
        try {
        	 log.info("Error converting hit to target class", response);
        	 List<HitEnvelope<T>> wrapped = response.hits().hits().stream()
        	            .map(hit -> {
        	                try {
        	                    T data;
        	                    Map<String, Object> src = hit.source();
        	                    if (src == null) return null; // skip if no _source

        	                    if (targetClass == Map.class || targetClass == Object.class) {
        	                        data = (T) src;
        	                    } else {
        	                        data = objectMapper.convertValue(src, targetClass);
        	                    }

        	                    return HitEnvelope.<T>builder()
        	                            .id(hit.id())
        	                            .score(hit.score()) // may be null if scores aren’t tracked
        	                            .data(data)
        	                            .build();

        	                } catch (Exception e) {
        	                    log.error("Error converting hit to target class {}", targetClass, e);
        	                    return null;
        	                }
        	            })
        	            .filter(Objects::nonNull)
        	            .collect(Collectors.toList());
            
            Integer pageSize = (Integer) esQuery.getOrDefault("size", 10);
            Integer from = (Integer) esQuery.getOrDefault("from", 0);
            Integer currentPage = pageSize == 0 ? 0 : from / pageSize;
            long totalHits = response.hits().total() != null ? response.hits().total().value() : 0L;

            return SearchResult.<T>builder().success(Boolean.TRUE)
                    .results(wrapped)
                    .totalHits(totalHits)
                    .took(response.took())
                    .hasMore(pageSize != 0 && wrapped.size() == pageSize && from + pageSize < totalHits)
                    .currentPage(currentPage)
                    .pageSize(pageSize)
                    .build();
        } catch (Exception e) {
            log.error("Error converting Elasticsearch response", e);
            throw new RuntimeException("Response conversion failed", e);
        }
    }

    private ElasticsearchResponse<Map<String, Object>> convertToElasticsearchResponse(SearchResponse<Map> response) {
        List<ElasticsearchResponse.Hits.Hit<Map<String, Object>>> hits = response.hits().hits().stream()
                .map(hit -> ElasticsearchResponse.Hits.Hit.<Map<String, Object>>builder()
                        .index(hit.index())
                        .id(hit.id())
                        .score(hit.score())
                        .source((Map<String, Object>) hit.source())
                        .build())
                .collect(Collectors.toList());

        ElasticsearchResponse.Hits.HitsTotal total = ElasticsearchResponse.Hits.HitsTotal.builder()
                .value(response.hits().total() != null ? response.hits().total().value() : 0L)
                .relation("eq")
                .build();

        ElasticsearchResponse.Hits<Map<String, Object>> hitsContainer = ElasticsearchResponse.Hits.<Map<String, Object>>builder()
                .total(total)
                .maxScore(response.hits().hits().stream()
                        .filter(hit -> hit.score() != null)
                        .mapToDouble(Hit::score)
                        .max()
                        .orElse(0.0))
                .hits(hits)
                .build();

        return ElasticsearchResponse.<Map<String, Object>>builder()
                .took(response.took())
                .timedOut(response.timedOut())
                .hits(hitsContainer)
                .build();
    }

    private FieldValue toFieldValue(Object value) {
        if (value == null) {
            return FieldValue.of(JsonData.of((Object) null));
        }
        if (value instanceof String s) return FieldValue.of(s);
        if (value instanceof Integer i) return FieldValue.of(i.longValue());
        if (value instanceof Long l) return FieldValue.of(l);
        if (value instanceof Short s) return FieldValue.of((long) s);
        if (value instanceof Byte b) return FieldValue.of((long) b);
        if (value instanceof Double d) return FieldValue.of(d);
        if (value instanceof Float f) return FieldValue.of((double) f);
        if (value instanceof Boolean b) return FieldValue.of(b);
        return FieldValue.of(JsonData.of(value));
    }
}
