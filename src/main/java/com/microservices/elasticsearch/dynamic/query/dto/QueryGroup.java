package com.microservices.elasticsearch.dynamic.query.dto;

import java.util.List;
import java.util.Map;

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

    // Allow either conditions or inline sub-groups inside conditions[] by accepting maps
    @Valid
    private List<Map<String, Object>> conditions;

    @Valid
    private List<Map<String, Object>> groups;

    @JsonProperty("nested_path")
    private String nestedPath;

    // Also accept "has_child" from payload; mapping handled in builder
    @JsonProperty("has_child_type")
    private String hasChildType;

    private Boolean negate;
}
