/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.iceberg;

import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Utility class for handling Iceberg column alias configuration.
 *
 * <p>Column aliases are configured via table options with the pattern: {@code
 * metadata.iceberg.column.alias.<column_name> = <alias_value>}
 *
 * <p>These aliases are used to generate the Iceberg name-mapping property, which allows multiple
 * names to map to the same field ID.
 */
public class IcebergColumnAliasOptions {

    /** Prefix for column alias options. */
    public static final String PREFIX = "metadata.iceberg.column.alias.";

    /** Table property key for name-mapping in Iceberg metadata. */
    public static final String NAME_MAPPING_PROPERTY = "schema.name-mapping.default";

    /**
     * Pattern for valid Iceberg identifiers: starts with letter or underscore, followed by
     * alphanumerics/underscores.
     */
    private static final Pattern VALID_IDENTIFIER = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    /**
     * Parse column aliases from table options.
     *
     * @param options table options
     * @return map of column name to alias
     */
    public static Map<String, String> parseAliases(Map<String, String> options) {
        Map<String, String> aliases = new HashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(PREFIX)) {
                // Extract column name from: metadata.iceberg.column.alias.<column_name>
                String columnName = key.substring(PREFIX.length());
                if (!columnName.isEmpty()) {
                    aliases.put(columnName, entry.getValue());
                }
            }
        }
        return aliases;
    }

    /**
     * Build the Iceberg name-mapping JSON string.
     *
     * <p>The name-mapping format is:
     *
     * <pre>
     * [
     *   {"field-id": 0, "names": ["original_name", "alias"]},
     *   {"field-id": 1, "names": ["another_field"]}
     * ]
     * </pre>
     *
     * @param fields list of data fields
     * @param columnAliases map of column name to alias
     * @return JSON string representation of the name-mapping
     */
    public static String buildNameMapping(
            List<DataField> fields, Map<String, String> columnAliases) {
        List<NameMappingEntry> entries = new ArrayList<>();
        for (DataField field : fields) {
            List<String> names = new ArrayList<>();
            names.add(field.name());

            String alias = columnAliases.get(field.name());
            if (alias != null && !alias.equals(field.name())) {
                names.add(alias);
            }

            entries.add(new NameMappingEntry(field.id(), names));
        }
        return JsonSerdeUtil.toJson(entries);
    }

    /** Entry in the Iceberg name-mapping. */
    private static class NameMappingEntry {
        @JsonProperty("field-id")
        final int fieldId;

        @JsonProperty("names")
        final List<String> names;

        NameMappingEntry(int fieldId, List<String> names) {
            this.fieldId = fieldId;
            this.names = names;
        }
    }

    /**
     * Validate column aliases.
     *
     * @param fields list of data fields in the schema
     * @param columnAliases map of column name to alias
     * @throws IllegalArgumentException if validation fails
     */
    public static void validateAliases(List<DataField> fields, Map<String, String> columnAliases) {
        if (columnAliases.isEmpty()) {
            return;
        }

        // Build set of existing column names
        Set<String> existingColumns =
                fields.stream().map(DataField::name).collect(Collectors.toSet());

        // Track all aliases to detect duplicates
        Set<String> seenAliases = new HashSet<>();

        for (Map.Entry<String, String> entry : columnAliases.entrySet()) {
            String columnName = entry.getKey();
            String alias = entry.getValue();

            // Check that column exists
            if (!existingColumns.contains(columnName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Column '%s' specified in alias configuration does not exist in the schema",
                                columnName));
            }

            // Check that alias is not empty
            if (alias == null || alias.isEmpty()) {
                throw new IllegalArgumentException(
                        String.format("Alias for column '%s' cannot be empty", columnName));
            }

            // Check that alias is a valid identifier
            if (!VALID_IDENTIFIER.matcher(alias).matches()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Alias '%s' for column '%s' is not a valid identifier. "
                                        + "Identifiers must start with a letter or underscore and contain only "
                                        + "letters, digits, and underscores.",
                                alias, columnName));
            }

            // Skip further checks if alias equals the column's own name (redundant but valid)
            if (alias.equals(columnName)) {
                continue;
            }

            // Check that alias doesn't conflict with existing column names
            if (existingColumns.contains(alias)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Alias '%s' for column '%s' conflicts with an existing column name",
                                alias, columnName));
            }

            // Check for duplicate aliases
            if (seenAliases.contains(alias)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Alias '%s' is a duplicate - the same alias cannot be used for multiple columns",
                                alias));
            }
            seenAliases.add(alias);
        }
    }
}
