package com.microservices.elasticsearch.dynamic.query.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class HitEnvelope<T> {
    private String id;     // _id
    private Double score;  // _score (can be null if not scored)
    private T data;        // _source mapped to T
}
