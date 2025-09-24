package com.microservices.elasticsearch.dynamic.query.dto;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryStructureRequest {
    @NotNull
    @Valid
    private QueryGroup query;
    
    @Valid
    private PaginationRequest pagination;
    
    @JsonProperty("_source")
    private List<String> sourceFields;
}











