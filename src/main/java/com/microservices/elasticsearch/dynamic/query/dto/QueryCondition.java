package com.microservices.elasticsearch.dynamic.query.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryCondition {
    @NotBlank
    private String field;
    
    @NotBlank
    private String operator; // ==, !=, >, >=, <, <=, range, wildcard, match, in, between, exists, missing
    
    private Object value;
    
    private Double boost;
    
    @JsonProperty("field_type")
    private String fieldType;
}
