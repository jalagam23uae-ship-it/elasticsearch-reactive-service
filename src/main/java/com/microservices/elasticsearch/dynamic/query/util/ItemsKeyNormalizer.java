package com.microservices.elasticsearch.dynamic.query.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public final class ItemsKeyNormalizer {
  private ItemsKeyNormalizer() {}

  /** Mutates and returns root. Looks for any "fields":[{...}] and renames keys containing "_items." */
  public static JsonNode normalize(JsonNode root) {
    if (root == null || !root.isObject()) return root;
    walkObject((ObjectNode) root);
    return root;
  }

  private static void walkObject(ObjectNode obj) {
    // fix a direct "fields" array if present
    JsonNode fields = obj.get("fields");
    if (fields instanceof ArrayNode arr) {
      for (JsonNode n : arr) if (n instanceof ObjectNode g) renameGroupKeys(g);
    }
    // recurse all children
    obj.fields().forEachRemaining(e -> {
      JsonNode v = e.getValue();
      if (v.isObject()) walkObject((ObjectNode) v);
      else if (v.isArray()) {
        for (JsonNode a : v) if (a.isObject()) walkObject((ObjectNode) a);
      }
    });
  }

  private static void renameGroupKeys(ObjectNode group) {
    List<String> remove = new ArrayList<>();
    Map<String, JsonNode> add = new LinkedHashMap<>();

    group.fields().forEachRemaining(entry -> {
      String key = entry.getKey();
      if ("operator".equals(key)) return;
      // only strip when segment is followed by a dot
      if (key.contains("_items.")) {
        String newKey = key.replace("_items.", "."); // e.g., customer_addresses_items.address_type -> customer_addresses.address_type
        if (!group.has(newKey)) add.put(newKey, entry.getValue());
        remove.add(key);
      }
    });

    remove.forEach(group::remove);
    add.forEach(group::set);
  }
}


