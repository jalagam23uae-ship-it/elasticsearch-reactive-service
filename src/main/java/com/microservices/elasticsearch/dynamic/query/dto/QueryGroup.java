package com.microservices.elasticsearch.dynamic.query.dto;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryGroup {
    @NotBlank
    private String operator; // AND, OR, NOT
    
    @Valid
    private List<QueryCondition> conditions;
    
    @Valid
    private List<QueryGroup> groups;
    
    @JsonProperty("nested_path")
    private String nestedPath;
    
    @JsonProperty("has_child_type")
    private String hasChildType;
    
    private Boolean negate;
}