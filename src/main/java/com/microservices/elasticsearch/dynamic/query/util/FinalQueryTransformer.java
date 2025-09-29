package com.microservices.elasticsearch.dynamic.query.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
        //finalOut.put("index_name", indexName);

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
                                            arrayOf(conditionv1(target.fieldPath(), op, v)));
                                    groupItems.add(ng);
                                }
                            } else {
                                // OR => one nested group with array
                                ObjectNode ng = nestedGroup("OR", target.scope(),
                                        arrayOf(conditionv1(target.fieldPath(), op, values)));
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
    
    private static ObjectNode conditionv1(String fieldPath, String operator, String singleValue) {
        ObjectNode n = M.createObjectNode();
        n.put("field", fieldPath+".keyword");
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
    
    
    
    private static ObjectNode conditionv1(String fieldPath, String operator, List<String> values) {
        ObjectNode n = M.createObjectNode();
        n.put("field", fieldPath+".keyword");
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
    
    
    
    public enum FieldRole { ROOT, NESTED, JOIN }

    public record TableRelation(
            String parentTable,
            String parentField,
            String childTable,
            String childField,
            String type,      // "nested" | "join" (case-insensitive)
            long id,
            String source
    ) {}

    public record Column(
            String name,
            String type,
            Integer length,
            boolean nullable
    ) {}

    // ----- Public API -----

    /**
     * Build a map of fully-qualified field -> role ("root" | "nested" | "join").
     * Rules:
     *  - All columns of every parentTable across relations are marked "root".
     *  - All columns of each childTable are marked by relation.type ("nested" or "join").
     *  - Role precedence: root > join > nested (higher precedence overwrites lower).
     *
     * @param relations    List of table relations.
     * @param tableColumns Map: tableName -> list of columns in that table.
     * @return Map of "table.field" (lowercase) -> "root"/"join"/"nested".
     */
    public static Map<String, String> buildFieldRoleMap(
            List<TableRelation> relations,
            Map<String, List<Column>> tableColumns
    ) {
        // Use LinkedHashMap for stable iteration order.
        Map<String, FieldRole> typed = buildFieldRoleMapTyped(relations, tableColumns);

        // Convert to requested string roles
        Map<String, String> out = new LinkedHashMap<>(typed.size());
        for (var e : typed.entrySet()) {
            out.put(e.getKey(), switch (e.getValue()) {
                case ROOT -> "root";
                case JOIN -> "join";
                case NESTED -> "nested";
            });
        }
        return out;
    }

    /**
     * Same as buildFieldRoleMap but returns typed roles.
     */
    public static Map<String, FieldRole> buildFieldRoleMapTyped(
            List<TableRelation> relations,
            Map<String, List<Column>> tableColumns
    ) {
        Map<String, FieldRole> result = new LinkedHashMap<>();

        // Index tables -> columns with normalized lower-case table/column names
        Map<String, List<String>> normalizedTableToCols = new HashMap<>();
        for (var entry : tableColumns.entrySet()) {
            String tbl = norm(entry.getKey());
            List<String> cols = entry.getValue() == null ? List.of()
                    : entry.getValue().stream()
                            .map(c -> norm(c.name()))
                            .toList();
            normalizedTableToCols.put(tbl, cols);
        }

        // Helper to apply a role to all columns of a table with precedence
        final var applyRoleToTable = new Object() {
            void apply(String rawTableName, FieldRole role) {
                String tbl = norm(rawTableName);
                List<String> cols = normalizedTableToCols.getOrDefault(tbl, List.of());
                for (String col : cols) {
                    String fq = tbl + "." + col;
                    upsertWithPrecedence(result, fq, role);
                }
            }
        };

        // First pass: mark all parentTable columns as ROOT
        for (var rel : relations) {
            applyRoleToTable.apply(rel.parentTable(), FieldRole.ROOT);
        }

        // Second pass: mark child tables by relation type (JOIN or NESTED)
        for (var rel : relations) {
            FieldRole role = roleFromType(rel.type());
            applyRoleToTable.apply(rel.childTable(), role);
        }

        return result;
    }

    // ----- Utilities -----
    
    
    

    private static String norm(String s) {
        return s == null ? "" : s.trim().toLowerCase(Locale.ROOT);
    }

    private static FieldRole roleFromType(String t) {
        String x = norm(t);
        return switch (x) {
            case "join" -> FieldRole.JOIN;
            case "nested" -> FieldRole.NESTED;
            default -> throw new IllegalArgumentException("Unknown relation type: " + t);
        };
    }

    // precedence: ROOT(3) > JOIN(2) > NESTED(1)
    private static final Map<FieldRole, Integer> ROLE_WEIGHT = Map.of(
            FieldRole.ROOT, 3,
            FieldRole.JOIN, 2,
            FieldRole.NESTED, 1
    );

    private static void upsertWithPrecedence(Map<String, FieldRole> map, String key, FieldRole incoming) {
        FieldRole existing = map.get(key);
        if (existing == null || ROLE_WEIGHT.get(incoming) > ROLE_WEIGHT.get(existing)) {
            map.put(key, incoming);
        }
    }
    
    public static Map<String, String> transformByRole(Map<String, String> fieldRoleMap) {
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : fieldRoleMap.entrySet()) {
            String key = e.getKey();
            String role = e.getValue() == null ? "" : e.getValue().trim().toLowerCase(Locale.ROOT);

            String value;
            switch (role) {
                case "root": {
                    int dot = key.indexOf('.');
                    value = (dot >= 0 && dot + 1 < key.length()) ? key.substring(dot + 1) : key;
                    break;
                }
                case "nested": {
                    value = key.replace(".", "_items."); // same as key
                    break;
                }
                case "join": {
                    value = key.replace('.', '#'); // replace '.' with '#'
                    break;
                }
                default:
                    throw new IllegalArgumentException("Unknown role '" + e.getValue() + "' for key: " + key);
            }
            out.put(key, value);
        }
        return out;
    }
    
    
    
    
    
    
    public static void main(String[] args) throws Exception {
    	
    	 Map<String, List<Column>> tableColumns = new HashMap<>();
         tableColumns.put("CUSTOMER_ADDRESSES", List.of(
                 new Column("ADDRESS_ID", "NUMBER", 22, true),
                 new Column("CUSTOMER_ID", "NUMBER", 22, true),
                 new Column("ADDRESS_TYPE", "VARCHAR2", 20, true),
                 new Column("STREET_ADDRESS", "VARCHAR2", 200, true),
                 new Column("CITY", "VARCHAR2", 100, true),
                 new Column("STATE_PROVINCE", "VARCHAR2", 100, true),
                 new Column("POSTAL_CODE", "VARCHAR2", 20, true),
                 new Column("COUNTRY", "VARCHAR2", 100, true),
                 new Column("IS_DEFAULT", "CHAR", 1, true),
                 new Column("CREATED_DATE", "DATE", 7, true)
         ));
         tableColumns.put("CUSTOMER_PREFERENCES", List.of(
                 new Column("PREFERENCE_ID", "NUMBER", 22, true),
                 new Column("CUSTOMER_ID", "NUMBER", 22, true),
                 new Column("PREFERENCE_CATEGORY", "VARCHAR2", 50, true),
                 new Column("PREFERENCE_KEY", "VARCHAR2", 100, true),
                 new Column("PREFERENCE_VALUE", "VARCHAR2", 500, true),
                 new Column("CREATED_DATE", "DATE", 7, true)
         ));
         tableColumns.put("CUSTOMERS", List.of(
                 new Column("CUSTOMER_ID", "NUMBER", 22, false),
                 new Column("FIRST_NAME", "VARCHAR2", 50, false),
                 new Column("LAST_NAME", "VARCHAR2", 50, false),
                 new Column("EMAIL", "VARCHAR2", 100, false),
                 new Column("PHONE", "VARCHAR2", 20, true),
                 new Column("DATE_OF_BIRTH", "DATE", 7, true),
                 new Column("GENDER", "CHAR", 1, true),
                 new Column("REGISTRATION_DATE", "DATE", 7, true),
                 new Column("LAST_LOGIN_DATE", "TIMESTAMP(6)", 11, true),
                 new Column("CUSTOMER_STATUS", "VARCHAR2", 20, true),
                 new Column("TOTAL_ORDERS", "NUMBER", 22, true),
                 new Column("TOTAL_SPENT", "NUMBER", 22, true),
                 new Column("LOYALTY_POINTS", "NUMBER", 22, true),
                 new Column("PREFERRED_LANGUAGE", "VARCHAR2", 10, true),
                 new Column("MARKETING_CONSENT", "CHAR", 1, true),
                 new Column("CREATED_BY", "VARCHAR2", 50, true),
                 new Column("CREATED_DATE", "DATE", 7, true),
                 new Column("MODIFIED_BY", "VARCHAR2", 50, true),
                 new Column("MODIFIED_DATE", "DATE", 7, true)
         ));

         // Build sample tablerelation (from your JSON)
         List<TableRelation> relations = List.of(
                 new TableRelation("CUSTOMERS", "CUSTOMER_ID", "CUSTOMER_ADDRESSES", "CUSTOMER_ID", "nested", 1758626968585L, "manual"),
                 new TableRelation("CUSTOMERS", "CUSTOMER_ID", "CUSTOMER_PREFERENCES", "CUSTOMER_ID", "nested",   1758627021385L, "manual")
         );

         Map<String, String> roleMap = buildFieldRoleMap(relations, tableColumns);
         
         Map<String, String> finalMap = transformByRole(roleMap);
         finalMap.forEach((k, v) -> System.out.println(k + " -> " + v));
         
         
    	
        String inputJson = """
        {
          "operator": "OR",
          "fields": [
            {
              "operator": "AND",
              "customers.first_name": { "value": "test", "op": "match" },
              "customers.gender":     { "value": "M",    "op": "match" },
              "customer_addresses.city": {
                "type": "multi_value",
                 "operator": "OR", "op": "in",
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

      
        JsonNode input = M.readTree(inputJson);
        ObjectNode out = transform(input, finalMap, "customer_addresses_index_dev2", 0, 10, "desc");
        System.out.println(M.writerWithDefaultPrettyPrinter().writeValueAsString(out));
    }
}

