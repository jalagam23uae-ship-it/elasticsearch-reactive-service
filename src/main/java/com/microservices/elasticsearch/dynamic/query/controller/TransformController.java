package com.microservices.elasticsearch.dynamic.query.controller;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.microservices.elasticsearch.dynamic.query.dto.ElasticsearchQueryRequest;
import com.microservices.elasticsearch.dynamic.query.dto.SearchResult;
import com.microservices.elasticsearch.dynamic.query.dto.TransformRequest;
import com.microservices.elasticsearch.dynamic.query.service.ElasticsearchService;
import com.microservices.elasticsearch.dynamic.query.service.QueryTransformService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/api/elasticsearch/v1")
@RequiredArgsConstructor
public class TransformController {

	private final QueryTransformService transformService;
	private final ElasticsearchService elasticsearchService;

	@PostMapping("/search")
	public CompletableFuture<ResponseEntity<SearchResult<Map>>> buildFinalQuery(
			@Valid @RequestBody TransformRequest request,@RequestHeader Map<String, String> headers) {
		String mappingName=headers.get("service_id");
		log.info("Transforming request buildFinalQuery  mapping: {}", mappingName);
		log.info("Transforming request buildFinalQuery  getQuery: {}", request.getQuery());
		ElasticsearchQueryRequest out = transformService.buildFinalQuery(request,mappingName);
		log.info("Transforming request for out: {}", out);
		return elasticsearchService.searchWithVirtualThreads(out.getIndexName(), out, Map.class)
				.thenApply(ResponseEntity::ok).exceptionally(throwable -> {
					log.error("Virtual thread search failed for index: {}", mappingName, throwable);
					return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
				});
	}

	@ExceptionHandler(IllegalArgumentException.class)
	public ResponseEntity<String> handleBadRequest(IllegalArgumentException ex) {
		return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ex.getMessage());
	}
}
