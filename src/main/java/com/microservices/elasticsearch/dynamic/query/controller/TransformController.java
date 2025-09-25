package com.microservices.elasticsearch.dynamic.query.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microservices.elasticsearch.dynamic.query.dto.TransformRequest;
import com.microservices.elasticsearch.dynamic.query.service.QueryTransformService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/api/transform")
@RequiredArgsConstructor
public class TransformController {

    private final QueryTransformService transformService;

    @PostMapping("/final-query")
    public ResponseEntity<ObjectNode> buildFinalQuery(@Valid @RequestBody TransformRequest request) {
        log.info("Transforming request for mapping: {}", request.getMappingName());
        ObjectNode out = transformService.buildFinalQuery(request.getMappingName(), request.getQuery());
        return ResponseEntity.ok(out);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ex.getMessage());
    }
}

