package com.microservices.elasticsearch.dynamic.query.dto;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AggregationRequest {
    private String name;
    private String type; // terms, avg, sum
    private String field;
    
    @JsonProperty("nested_path")
    private String nestedPath;
    
    @JsonProperty("sub_aggregations")
    private List<AggregationRequest> subAggregations;
}
