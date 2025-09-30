package com.microservices.elasticsearch.dynamic.query.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ElasticsearchQueryRequest {
	@NotNull
	@Valid
	private QueryStructureRequest queryStructure;

	@Valid
	private QueryAggregationsRequest queryAggregations;

	private String indexName;

}
