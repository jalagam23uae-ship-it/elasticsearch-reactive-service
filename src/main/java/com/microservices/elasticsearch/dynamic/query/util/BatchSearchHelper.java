package com.microservices.elasticsearch.dynamic.query.util;

import java.util.List;
import java.util.Map;

import com.microservices.elasticsearch.dynamic.query.dto.ElasticsearchQueryRequest;
import com.microservices.elasticsearch.dynamic.query.dto.SearchRequest;

/**
 * Helper utility for creating batch search requests
 */
public class BatchSearchHelper {
    
    /**
     * Create a batch search request for Map<String, Object> results
     */
	
	
    public static SearchRequest<Map> createMapSearchRequest(
            String indexName, 
            ElasticsearchQueryRequest queryRequest) {
        return SearchRequest.<Map>builder()
                .indexName(indexName)
                .queryRequest(queryRequest)
                .targetClass(Map.class)
                .build();
    }
    
    /**
     * Create a batch search request for a specific type
     */
    public static <T> SearchRequest<T> createTypedSearchRequest(
            String indexName, 
            ElasticsearchQueryRequest queryRequest,
            Class<T> targetClass) {
        return SearchRequest.<T>builder()
                .indexName(indexName)
                .queryRequest(queryRequest)
                .targetClass(targetClass)
                .build();
    }
    
    /**
     * Create multiple search requests for the same query across different indices
     */
    public static List<SearchRequest<Map>> createMultiIndexSearchRequests(
            List<String> indexNames,
            ElasticsearchQueryRequest queryRequest) {
        return indexNames.stream()
                .map(indexName -> createMapSearchRequest(indexName, queryRequest))
                .toList();
    }
}

