package com.microservices.elasticsearch.dynamic.query.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SearchRequest<T> {
    private ElasticsearchQueryRequest queryRequest;
    private String indexName;
    private Class<T> targetClass;
}
