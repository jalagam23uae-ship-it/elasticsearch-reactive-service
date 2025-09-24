package com.microservices.elasticsearch.dynamic.query.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ElasticsearchResponse<T> {
    private Long took;
    private Boolean timedOut;
    private Shards shards;
    private Hits<T> hits;
    private Map<String, Object> aggregations;
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Shards {
        private Integer total;
        private Integer successful;
        private Integer skipped;
        private Integer failed;
    }
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Hits<T> {
        private HitsTotal total;
        @JsonProperty("max_score")
        private Double maxScore;
        private List<Hit<T>> hits;
        
        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        public static class HitsTotal {
            private Long value;
            private String relation;
        }
        
        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        public static class Hit<T> {
            @JsonProperty("_index")
            private String index;
            @JsonProperty("_id")
            private String id;
            @JsonProperty("_score")
            private Double score;
            @JsonProperty("_source")
            private T source;
        }
    }
}



