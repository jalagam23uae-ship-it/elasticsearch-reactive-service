package com.microservices.elasticsearch.dynamic.query.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;

import java.util.*;

public class FinalQueryTransformer {

    // Jackson instance
    private static final ObjectMapper M = new ObjectMapper();

    // Kinds of mapped targets
    enum Kind { TOP, NESTED, PARENT }

    // Result of parsing a mapping value
    record Target(Kind kind, String scope, String fieldPath) {
        // kind:
        //  TOP    : scope = null,                 fieldPath = "esField"
        //  NESTED : scope = nested path,          fieldPath = "nested.path.field"
        //  PARENT : scope = parent type,          fieldPath = "parent.childField"
    }

    /**
     * Transform UI input JSON -> your normalized "finaloutput" JSON structure.
     *
     * @param input     inputrequest JSON (UI shape)
     * @param mapping   uiField -> mappingValue ('.' => nested, '#' => parent-child)
     * @param indexName index name to place in finaloutput
     * @param from      pagination from
     * @param size      pagination size
     * @param sortOrder "asc" or "desc"
     */
    public static ObjectNode transform(JsonNode input,
                                       Map<String, String> mapping,
                                       String indexName,
                                       int from,
                                       int size,
                                       String sortOrder) {

        // --- finaloutput root ---
        ObjectNode finalOut = M.createObjectNode();
        finalOut.put("index_name", indexName);

        // Top-level query object
        ObjectNode query = M.createObjectNode();
        query.put("operator", "AND"); // normalized to AND per your sample

        ArrayNode topConditions = M.createArrayNode(); // holds group objects for non-parent items
        ArrayNode topGroups     = M.createArrayNode(); // holds parent-child groups at top level

        // Iterate top-level "fields" array of UI
        ArrayNode groups = (ArrayNode) input.path("fields");
        for (JsonNode uiGroup : groups) {
            String groupOp = uiGroup.path("operator").asText("AND");

            // We'll collect conditions and nested groups together inside this group
            ArrayNode groupItems = M.createArrayNode();
            boolean hasNonParentItems = false;

            // Each entry in the group (skip the "operator" key)
            Iterator<Map.Entry<String, JsonNode>> it = uiGroup.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                String uiField = e.getKey();
                if ("operator".equalsIgnoreCase(uiField)) continue;

                // Resolve mapping
                String mapVal = mapping.get(uiField);
                if (mapVal == null) continue; // ignore unmapped

                Target target = parseTarget(mapVal);
                JsonNode condNode = e.getValue();

                boolean isMulti = "multi_value".equalsIgnoreCase(condNode.path("type").asText(""));
                String innerOp  = condNode.path("operator").asText(groupOp); // OR/AND inside the field; fallback to group op

                if (isMulti) {
                    // Multi-value: condNode.values (array), condNode.op (e.g., "in")
                    List<String> values = toStrings(condNode.path("values"));
                    String op = condNode.path("op").asText("in");

                    switch (target.kind()) {
                        case PARENT -> {
                            // Parent-child groups live at top-level "groups"
                            if ("AND".equalsIgnoreCase(innerOp)) {
                                // AND across values => multiple parent groups (each single value)
                                for (String v : values) {
                                    ObjectNode pc = parentGroup("AND", target.scope(),
                                            arrayOf(condition(target.fieldPath(), op, v)));
                                    topGroups.add(pc);
                                }
                            } else {
                                // OR (or default) => one parent group with array
                                ObjectNode pc = parentGroup("OR", target.scope(),
                                        arrayOf(condition(target.fieldPath(), op, values)));
                                topGroups.add(pc);
                            }
                        }
                        case NESTED -> {
                            hasNonParentItems = true;
                            if ("AND".equalsIgnoreCase(innerOp)) {
                                // AND across values => multiple nested groups, single value each
                                for (String v : values) {
                                    ObjectNode ng = nestedGroup("AND", target.scope(),
                                            arrayOf(condition(target.fieldPath(), op, v)));
                                    groupItems.add(ng);
                                }
                            } else {
                                // OR => one nested group with array
                                ObjectNode ng = nestedGroup("OR", target.scope(),
                                        arrayOf(condition(target.fieldPath(), op, values)));
                                groupItems.add(ng);
                            }
                        }
                        case TOP -> {
                            hasNonParentItems = true;
                            if ("AND".equalsIgnoreCase(innerOp)) {
                                // multiple single-value conditions
                                for (String v : values) {
                                    groupItems.add(condition(target.fieldPath(), op, v));
                                }
                            } else {
                                // one condition carrying array
                                groupItems.add(condition(target.fieldPath(), op, values));
                            }
                        }
                    }

                } else {
                    // Single value: condNode.value, condNode.op
                    String op = condNode.path("op").asText("match");
                    JsonNode valNode = condNode.get("value");

                    switch (target.kind()) {
                        case PARENT -> {
                            ObjectNode pc = parentGroup(innerOp, target.scope(),
                                    arrayOf(condition(target.fieldPath(), op, valNode)));
                            topGroups.add(pc);
                        }
                        case NESTED -> {
                            hasNonParentItems = true;
                            ObjectNode ng = nestedGroup(innerOp, target.scope(),
                                    arrayOf(condition(target.fieldPath(), op, valNode)));
                            groupItems.add(ng);
                        }
                        case TOP -> {
                            hasNonParentItems = true;
                            groupItems.add(condition(target.fieldPath(), op, valNode));
                        }
                    }
                }
            }

            if (hasNonParentItems) {
                ObjectNode groupNode = M.createObjectNode();
                groupNode.put("operator", groupOp);
                groupNode.set("groups", groupItems); // contains field-conditions and nested groups
                topConditions.add(groupNode);
            }
        }

        if (topConditions.size() > 0) query.set("conditions", topConditions);
        if (topGroups.size() > 0)     query.set("groups",     topGroups);
        finalOut.set("query", query);

        // pagination
        ObjectNode pagination = M.createObjectNode();
        pagination.put("from", from);
        pagination.put("size", size);
        finalOut.set("pagination", pagination);

        // sort
        ArrayNode sortArr = M.createArrayNode();
        ObjectNode sortObj = M.createObjectNode();
        sortObj.put("order", sortOrder);
        sortArr.add(sortObj);
        finalOut.set("sort", sortArr);

        return finalOut;
    }

    // --------- helpers ----------

    private static Target parseTarget(String mappingValue) {
        if (mappingValue.contains("#")) {
            String[] parts = mappingValue.split("#", 2);
            String parent = parts[0];
            String child  = parts[1];
            return new Target(Kind.PARENT, parent, parent + "." + child);
        } else if (mappingValue.contains(".")) {
            int dot = mappingValue.indexOf('.');
            String nestedPath = mappingValue.substring(0, dot);
            return new Target(Kind.NESTED, nestedPath, mappingValue);
        } else {
            return new Target(Kind.TOP, null, mappingValue);
        }
    }

    private static ObjectNode nestedGroup(String operator, String nestedPath, ArrayNode conditions) {
        ObjectNode g = M.createObjectNode();
        g.put("operator", operator);
        g.put("nested_path", nestedPath);
        g.set("conditions", conditions);
        return g;
    }

    private static ObjectNode parentGroup(String operator, String parent, ArrayNode conditions) {
        ObjectNode g = M.createObjectNode();
        g.put("operator", operator);
        g.put("has_child", parent);
        g.set("conditions", conditions);
        return g;
    }

    private static ObjectNode condition(String fieldPath, String operator, String singleValue) {
        ObjectNode n = M.createObjectNode();
        n.put("field", fieldPath);
        n.put("operator", operator);
        n.put("value", singleValue);
        return n;
    }

    private static ObjectNode condition(String fieldPath, String operator, List<String> values) {
        ObjectNode n = M.createObjectNode();
        n.put("field", fieldPath);
        n.put("operator", operator);
        ArrayNode arr = M.createArrayNode();
        for (String v : values) arr.add(v);
        n.set("value", arr);
        return n;
    }

    private static ObjectNode condition(String fieldPath, String operator, JsonNode rawValue) {
        ObjectNode n = M.createObjectNode();
        n.put("field", fieldPath);
        n.put("operator", operator);
        if (rawValue != null) n.set("value", rawValue.deepCopy());
        return n;
    }

    private static ArrayNode arrayOf(JsonNode... nodes) {
        ArrayNode a = M.createArrayNode();
        for (JsonNode n : nodes) a.add(n);
        return a;
    }

    private static List<String> toStrings(JsonNode values) {
        if (values == null || !values.isArray()) return List.of();
        List<String> out = new ArrayList<>(values.size());
        values.forEach(v -> out.add(v.asText()));
        return out;
    }

    // ---------- quick demo ----------
    public static void main(String[] args) throws Exception {
        String inputJson = """
        {
          "operator": "OR",
          "fields": [
            {
              "operator": "AND",
              "customers.first_name": { "value": "test", "op": "match" },
              "customers.gender":     { "value": "M",    "op": "match" },
              "customer_addresses.city": {
                "type": "multi_value", "operator": "OR", "op": "in",
                "values": ["New York","Chicago"]
              }
            },
            {
              "operator": "AND",
              "customer_preferences.preference_category": {
                "type": "multi_value", "operator": "AND", "op": "match",
                "values": ["COMMUNICATION","SHOPPING"]
              }
            }
          ]
        }
        """;

        Map<String, String> mapping = Map.of(
            "customers.first_name", "first_name",
            "customers.gender", "gender",
            "customer_addresses.city", "customer_addresses.city",
            "customer_preferences.preference_category", "customer_preferences.preference_category"
        );

        JsonNode input = M.readTree(inputJson);
        ObjectNode out = transform(input, mapping, "customer_addresses_index_dev2", 0, 10, "desc");
        System.out.println(M.writerWithDefaultPrettyPrinter().writeValueAsString(out));
    }
}

