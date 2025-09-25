package com.microservices.elasticsearch.dynamic.query.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microservices.elasticsearch.dynamic.query.dto.ElasticsearchQueryRequest;
import com.microservices.elasticsearch.dynamic.query.dto.QueryStructureRequest;
import com.microservices.elasticsearch.dynamic.query.util.FinalQueryTransformer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class QueryTransformService {

	private final WorkflowMappingsCache cache;
	private final ObjectMapper objectMapper = new ObjectMapper();

	public ElasticsearchQueryRequest buildFinalQuery(String mappingName, JsonNode inputQuery) {
		Map<String, Object> mappingRow = cache.get(mappingName);
		log.info("Transforming request for mappingRow: {}", mappingRow);
		if (mappingRow == null) {
			throw new IllegalArgumentException("Mapping not found: " + mappingName);
		}
		String indexName = stringValue(mappingRow, "index_name");
		if (indexName == null || indexName.isBlank()) {
			throw new IllegalArgumentException("index_name missing for mapping: " + mappingName);
		}
		log.info("Transforming request for indexName: {}", indexName);

		List<FinalQueryTransformer.TableRelation> relations = parseRelations(
				mappingRow.get(key(mappingRow, "relationships")));
		log.info("Transforming request for relations: {}", relations);
		Map<String, List<FinalQueryTransformer.Column>> tableColumns = parseTableStructures(
				mappingRow.get(key(mappingRow, "table_structures")));
		log.info("Transforming request for tableColumns: {}", tableColumns);
		Map<String, String> roleMap = FinalQueryTransformer.buildFieldRoleMap(relations, tableColumns);
		log.info("Transforming request for roleMap: {}", roleMap);
		Map<String, String> finalMap = FinalQueryTransformer.transformByRole(roleMap);
		log.info("Transforming request for finalMap: {}", finalMap);
		try {
			ObjectNode out = FinalQueryTransformer.transform(inputQuery, finalMap, indexName, 0, 10, "desc");
			String requestQuery = objectMapper.writeValueAsString(out);
			log.info("Transforming request for requestQuery: {}", requestQuery);
			QueryStructureRequest queryStructureRequest = objectMapper.readValue(requestQuery,
					QueryStructureRequest.class);
			 System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(out));
			 
			ElasticsearchQueryRequest queryRequest = new ElasticsearchQueryRequest(queryStructureRequest, null);
			return queryRequest;
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}

	}

	private String key(Map<String, Object> row, String desired) {
		String d = desired.toLowerCase(Locale.ROOT);
		for (String k : row.keySet()) {
			if (k != null && k.toLowerCase(Locale.ROOT).equals(d))
				return k;
		}
		return desired;
	}

	private String stringValue(Map<String, Object> row, String key) {
		Object v = row.get(key(row, key));
		return v == null ? null : String.valueOf(v);
	}

	private List<FinalQueryTransformer.TableRelation> parseRelations(Object src) {
		try {
			if (src == null)
				return List.of();
			if (src instanceof String s) {
				JsonNode node = objectMapper.readTree(s);
				List<FinalQueryTransformer.TableRelation> out = new ArrayList<>();
				if (node.isArray()) {
					for (JsonNode n : node) {
						out.add(new FinalQueryTransformer.TableRelation(text(n, "parentTable"), text(n, "parentField"),
								text(n, "childTable"), text(n, "childField"), text(n, "type"), n.path("id").asLong(0L),
								text(n, "source")));
					}
				}
				return out;
			}
			// already mapped structure? treat as list of maps
			if (src instanceof List<?> list) {
				List<FinalQueryTransformer.TableRelation> out = new ArrayList<>();
				for (Object o : list) {
					if (o instanceof Map<?, ?> m) {
						out.add(new FinalQueryTransformer.TableRelation(str(m.get("parentTable")),
								str(m.get("parentField")), str(m.get("childTable")), str(m.get("childField")),
								str(m.get("type")), toLong(m.get("id")), str(m.get("source"))));
					}
				}
				return out;
			}
		} catch (Exception e) {
			log.warn("Failed to parse relationships: {}", e.getMessage());
		}
		return List.of();
	}

	private Map<String, List<FinalQueryTransformer.Column>> parseTableStructures(Object src) {
		try {
			if (src == null)
				return Map.of();
			JsonNode root;
			if (src instanceof String s) {
				root = objectMapper.readTree(s);
			} else {
				root = objectMapper.valueToTree(src);
			}

			Map<String, List<FinalQueryTransformer.Column>> out = new HashMap<>();
			if (root.isObject()) {
				root.fields().forEachRemaining(e -> {
					String table = e.getKey();
					JsonNode arr = e.getValue();
					List<FinalQueryTransformer.Column> cols = new ArrayList<>();
					if (arr.isArray()) {
						for (JsonNode c : arr) {
							cols.add(new FinalQueryTransformer.Column(text(c, "name"), text(c, "type"),
									c.path("length").isNull() ? null : c.path("length").asInt(),
									c.path("nullable").asBoolean(true)));
						}
					}
					out.put(table, cols);
				});
			}
			return out;
		} catch (Exception e) {
			log.warn("Failed to parse table_structures: {}", e.getMessage());
			return Map.of();
		}
	}

	private static String text(JsonNode n, String field) {
		JsonNode v = n.get(field);
		if (v == null) {
			// try case-insensitive
			for (var it = n.fieldNames(); it.hasNext();) {
				String k = it.next();
				if (k.equalsIgnoreCase(field))
					return n.get(k).asText();
			}
			return null;
		}
		return v.asText();
	}

	private static String str(Object o) {
		return o == null ? null : String.valueOf(o);
	}

	private static long toLong(Object o) {
		if (o == null)
			return 0L;
		if (o instanceof Number n)
			return n.longValue();
		try {
			return Long.parseLong(String.valueOf(o));
		} catch (Exception e) {
			return 0L;
		}
	}
}
