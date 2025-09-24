package com.microservices.elasticsearch.dynamic.query.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SortSpec {
    // Optional: when absent we ignore this sort entry
    private String field;

    // asc | desc (defaults to asc if null)
    private String order;
}
