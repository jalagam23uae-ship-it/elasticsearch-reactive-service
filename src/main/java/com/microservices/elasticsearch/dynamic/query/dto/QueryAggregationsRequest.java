package com.microservices.elasticsearch.dynamic.query.dto;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryAggregationsRequest {
    private List<AggregationRequest> aggregations;
}
