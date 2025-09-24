package com.microservices.elasticsearch.dynamic.query.dto;

import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SearchResult<T> {
    private List<T> documents;
    private Long totalHits;
    private Map<String, Object> aggregations;
    private Long took;
    private Boolean hasMore;
    private Integer currentPage;
    private Integer pageSize;
}
