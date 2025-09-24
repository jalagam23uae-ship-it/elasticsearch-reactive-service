package com.microservices.elasticsearch.dynamic.query.service;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Service;

import com.microservices.elasticsearch.dynamic.query.dto.AggregationRequest;
import com.microservices.elasticsearch.dynamic.query.dto.ElasticsearchQueryRequest;
import com.microservices.elasticsearch.dynamic.query.dto.QueryCondition;
import com.microservices.elasticsearch.dynamic.query.dto.QueryGroup;
import com.microservices.elasticsearch.dynamic.query.exception.InvalidOperatorException;
import com.microservices.elasticsearch.dynamic.query.exception.InvalidValueException;

import lombok.extern.slf4j.Slf4j;

/**
 * Service that builds Elasticsearch queries from structured query requests
 * This is the Java translation of your Python query builder
 */
@Slf4j
@Service
public class ElasticsearchQueryBuilderService {
    
    /**
     * Build complete Elasticsearch query from request
     */
    public Map<String, Object> buildEsQuery(ElasticsearchQueryRequest request) {
        log.debug("Building Elasticsearch query from request");
        
        var queryStructure = request.getQueryStructure();
        var queryAggregations = request.getQueryAggregations();
        
        Map<String, Object> query = new HashMap<>();
        query.put("track_total_hits", true);
        query.put("query", groupToEs(queryStructure.getQuery(), null));
        
        // Handle pagination
        if (queryStructure.getPagination() != null) {
            var pagination = queryStructure.getPagination();
            query.put("from", pagination.getFrom());
            query.put("size", pagination.getSize());
        }
        
        // Handle aggregations
        if (queryAggregations != null && queryAggregations.getAggregations() != null) {
            query.put("aggs", buildEsAggregations(queryAggregations.getAggregations()));
        }
        
        // Handle source fields
        if (queryStructure.getSourceFields() != null && !queryStructure.getSourceFields().isEmpty()) {
            query.put("_source", queryStructure.getSourceFields());
        }
        
        log.debug("Built Elasticsearch query: {}", query);
        return query;
    }
    
    /**
     * Convert QueryCondition to Elasticsearch query clause
     */
    private Map<String, Object> conditionToEs(QueryCondition condition) {
        String field = condition.getField();
        String operator = condition.getOperator();
        Object value = condition.getValue();
        Double boost = condition.getBoost();
        String fieldType = Optional.ofNullable(condition.getFieldType()).orElse("keyword");
        
        return switch (operator) {
            case "==" -> {
                Map<String, Object> termQuery = new HashMap<>();
                Map<String, Object> fieldMap = new HashMap<>();
                fieldMap.put("value", value);
                if (boost != null) {
                    fieldMap.put("boost", boost);
                }
                termQuery.put(field, fieldMap);
                yield Map.of("term", termQuery);
            }
            case "!=" -> {
                Map<String, Object> termQuery = Map.of(field, Map.of("value", value));
                yield Map.of("bool", Map.of("must_not", List.of(Map.of("term", termQuery))));
            }
            case ">" -> Map.of("range", Map.of(field, Map.of("gt", value)));
            case ">=" -> Map.of("range", Map.of(field, Map.of("gte", value)));
            case "<" -> Map.of("range", Map.of(field, Map.of("lt", value)));
            case "<=" -> Map.of("range", Map.of(field, Map.of("lte", value)));
            case "range" -> {
                if (!(value instanceof Map<?, ?> valueMap)) {
                    throw new InvalidValueException("Range operator requires Map with 'gte' and/or 'lte' keys");
                }
                Map<String, Object> rangeQuery = new HashMap<>();
                valueMap.forEach((k, v) -> {
                    if (k instanceof String key && List.of("gte", "lte", "gt", "lt").contains(key)) {
                        rangeQuery.put(key, v);
                    }
                });
                yield Map.of("range", Map.of(field, rangeQuery));
            }
            case "wildcard" -> {
                Map<String, Object> wildcardQuery = new HashMap<>();
                Map<String, Object> fieldMap = new HashMap<>();
                fieldMap.put("value", value);
                if (boost != null) {
                    fieldMap.put("boost", boost);
                }
                wildcardQuery.put(field, fieldMap);
                yield Map.of("wildcard", wildcardQuery);
            }
            case "match" -> {
                Map<String, Object> matchQuery = new HashMap<>();
                Map<String, Object> fieldMap = new HashMap<>();
                fieldMap.put("query", value);
                if (boost != null) {
                    fieldMap.put("boost", boost);
                }
                matchQuery.put(field, fieldMap);
                yield Map.of("match", matchQuery);
            }
            case "in" -> {
                if (!(value instanceof List<?> valueList)) {
                    throw new InvalidValueException("The 'in' operator expects a list of values");
                }
                yield Map.of("terms", Map.of(field, valueList));
            }
            case "between" -> {
                if (!(value instanceof List<?> valueList) || valueList.size() != 2) {
                    throw new InvalidValueException("Between needs two values");
                }
                Map<String, Object> rangeQuery = new HashMap<>();
                rangeQuery.put("gte", valueList.get(0));
                rangeQuery.put("lte", valueList.get(1));
                if (List.of("date", "datetime").contains(fieldType)) {
                    rangeQuery.put("format", "strict_date_optional_time");
                }
                yield Map.of("range", Map.of(field, rangeQuery));
            }
            case "exists" -> Map.of("exists", Map.of("field", field));
            case "missing" -> Map.of("bool", Map.of("must_not", List.of(Map.of("exists", Map.of("field", field)))));
            default -> throw new InvalidOperatorException("Unsupported operator: " + operator);
        };
    }
    
    /**
     * Convert QueryGroup to Elasticsearch query group
     */
    private Map<String, Object> groupToEs(QueryGroup group, String inheritedNestedPath) {
        String logic = group.getOperator();
        String boolKey = switch (logic) {
            case "AND" -> "must";
            case "OR" -> "should";
            case "NOT" -> "must_not";
            default -> "must";
        };
        
        List<Map<String, Object>> clauses = new ArrayList<>();
        String currentNestedPath = Optional.ofNullable(group.getNestedPath()).orElse(inheritedNestedPath);
        String isHasChild = group.getHasChildType();
        
        // Process conditions
        if (group.getConditions() != null) {
            group.getConditions().forEach(condition -> 
                clauses.add(conditionToEs(condition))
            );
        }
        
        // Process subgroups
        if (group.getGroups() != null) {
            group.getGroups().forEach(subgroup -> {
                if (subgroup.getNestedPath() != null) {
                    clauses.add(groupToEs(subgroup, subgroup.getNestedPath()));
                } else if (subgroup.getHasChildType() != null) {
                    clauses.add(groupToEs(subgroup, subgroup.getHasChildType()));
                } else {
                    clauses.add(groupToEs(subgroup, currentNestedPath));
                }
            });
        }
        
        Map<String, Object> groupQuery = new HashMap<>();
        Map<String, Object> boolQuery = new HashMap<>();
        boolQuery.put(boolKey, clauses);
        
        if ("should".equals(boolKey)) {
            boolQuery.put("minimum_should_match", 1);
        }
        
        groupQuery.put("bool", boolQuery);
        
        // Handle has_child wrapping
        if (isHasChild != null) {
            groupQuery = Map.of("has_child", Map.of(
                "type", isHasChild,
                "query", groupQuery
            ));
        }
        // Handle nested wrapping
        else if (currentNestedPath != null) {
            groupQuery = Map.of("nested", Map.of(
                "path", currentNestedPath,
                "query", groupQuery
            ));
        }
        
        // Handle negation
        if (Boolean.TRUE.equals(group.getNegate())) {
            groupQuery = Map.of("bool", Map.of("must_not", List.of(groupQuery)));
        }
        
        return groupQuery;
    }
    
    /**
     * Build Elasticsearch aggregations from aggregation requests
     */
    private Map<String, Object> buildEsAggregations(List<AggregationRequest> aggsList) {
        Map<String, Object> esAggs = new HashMap<>();
        
        for (AggregationRequest agg : aggsList) {
            esAggs.put(agg.getName(), buildNestedAggs(agg));
        }
        
        return esAggs;
    }
    
    /**
     * Build nested aggregations recursively
     */
    private Map<String, Object> buildNestedAggs(AggregationRequest agg) {
        if (agg.getNestedPath() != null && !agg.getNestedPath().isEmpty()) {
            // Handle nested path and recursively build inner aggregation
            AggregationRequest innerAgg = AggregationRequest.builder()
                .name(agg.getName())
                .type(agg.getType())
                .field(agg.getField())
                .subAggregations(agg.getSubAggregations())
                .build();
            
            return Map.of(
                "nested", Map.of("path", agg.getNestedPath()),
                "aggs", Map.of(agg.getName(), buildNestedAggs(innerAgg))
            );
        }
        
        // Handle leaf aggregations
        Map<String, Object> result = switch (agg.getType()) {
            case "terms" -> Map.of("terms", Map.of("field", agg.getField()));
            case "avg" -> Map.of("avg", Map.of("field", agg.getField()));
            case "sum" -> Map.of("sum", Map.of("field", agg.getField()));
            case "count" -> Map.of("value_count", Map.of("field", agg.getField()));
            case "min" -> Map.of("min", Map.of("field", agg.getField()));
            case "max" -> Map.of("max", Map.of("field", agg.getField()));
            default -> throw new InvalidOperatorException("Unsupported aggregation type: " + agg.getType());
        };
        
        // Handle sub-aggregations
        if (agg.getSubAggregations() != null && !agg.getSubAggregations().isEmpty()) {
            Map<String, Object> subAggs = new HashMap<>();
            for (AggregationRequest sub : agg.getSubAggregations()) {
                subAggs.put(sub.getName(), buildNestedAggs(sub));
            }
            
            Map<String, Object> resultWithAggs = new HashMap<>(result);
            resultWithAggs.put("aggs", subAggs);
            result = resultWithAggs;
        }
        
        return result;
    }
}