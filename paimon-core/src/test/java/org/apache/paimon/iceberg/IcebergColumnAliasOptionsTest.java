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
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IcebergColumnAliasOptions}. */
public class IcebergColumnAliasOptionsTest {

    // ------------------------------------------------------------------------
    //  Alias Parsing Tests
    // ------------------------------------------------------------------------

    @Test
    public void testParseEmptyOptions() {
        Map<String, String> options = new HashMap<>();
        Map<String, String> aliases = IcebergColumnAliasOptions.parseAliases(options);
        assertThat(aliases).isEmpty();
    }

    @Test
    public void testParseUnrelatedOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "4");
        options.put("metadata.iceberg.storage", "table-location");
        Map<String, String> aliases = IcebergColumnAliasOptions.parseAliases(options);
        assertThat(aliases).isEmpty();
    }

    @Test
    public void testParseSingleAlias() {
        Map<String, String> options = new HashMap<>();
        options.put("metadata.iceberg.column.alias.user_id", "userId");
        Map<String, String> aliases = IcebergColumnAliasOptions.parseAliases(options);
        assertThat(aliases).hasSize(1);
        assertThat(aliases.get("user_id")).isEqualTo("userId");
    }

    @Test
    public void testParseMultipleAliases() {
        Map<String, String> options = new HashMap<>();
        options.put("metadata.iceberg.column.alias.user_id", "userId");
        options.put("metadata.iceberg.column.alias.created_at", "createdAt");
        options.put("metadata.iceberg.column.alias.order_total", "orderTotal");
        Map<String, String> aliases = IcebergColumnAliasOptions.parseAliases(options);
        assertThat(aliases).hasSize(3);
        assertThat(aliases.get("user_id")).isEqualTo("userId");
        assertThat(aliases.get("created_at")).isEqualTo("createdAt");
        assertThat(aliases.get("order_total")).isEqualTo("orderTotal");
    }

    @Test
    public void testParseMixedOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "4");
        options.put("metadata.iceberg.column.alias.user_id", "userId");
        options.put("metadata.iceberg.storage", "table-location");
        options.put("metadata.iceberg.column.alias.name", "userName");
        Map<String, String> aliases = IcebergColumnAliasOptions.parseAliases(options);
        assertThat(aliases).hasSize(2);
        assertThat(aliases.get("user_id")).isEqualTo("userId");
        assertThat(aliases.get("name")).isEqualTo("userName");
    }

    // ------------------------------------------------------------------------
    //  Name-Mapping Generation Tests
    // ------------------------------------------------------------------------

    @Test
    public void testBuildNameMappingNoAliases() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "name", DataTypes.STRING()));
        Map<String, String> aliases = Collections.emptyMap();

        String nameMapping = IcebergColumnAliasOptions.buildNameMapping(fields, aliases);

        // Should contain all fields with their names
        assertThat(nameMapping).contains("\"field-id\":0");
        assertThat(nameMapping).contains("\"field-id\":1");
        assertThat(nameMapping).contains("\"names\":[\"id\"]");
        assertThat(nameMapping).contains("\"names\":[\"name\"]");
    }

    @Test
    public void testBuildNameMappingWithAliases() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "user_id", DataTypes.INT()),
                        new DataField(1, "created_at", DataTypes.TIMESTAMP(6)));
        Map<String, String> aliases = new HashMap<>();
        aliases.put("user_id", "userId");
        aliases.put("created_at", "createdAt");

        String nameMapping = IcebergColumnAliasOptions.buildNameMapping(fields, aliases);

        // Should contain aliases alongside original names
        assertThat(nameMapping).contains("\"field-id\":0");
        assertThat(nameMapping).contains("\"names\":[\"user_id\",\"userId\"]");
        assertThat(nameMapping).contains("\"field-id\":1");
        assertThat(nameMapping).contains("\"names\":[\"created_at\",\"createdAt\"]");
    }

    @Test
    public void testBuildNameMappingPartialAliases() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "user_name", DataTypes.STRING()),
                        new DataField(2, "email", DataTypes.STRING()));
        Map<String, String> aliases = new HashMap<>();
        aliases.put("user_name", "userName");

        String nameMapping = IcebergColumnAliasOptions.buildNameMapping(fields, aliases);

        // Only user_name should have alias
        assertThat(nameMapping).contains("\"names\":[\"id\"]");
        assertThat(nameMapping).contains("\"names\":[\"user_name\",\"userName\"]");
        assertThat(nameMapping).contains("\"names\":[\"email\"]");
    }

    // ------------------------------------------------------------------------
    //  Validation Tests
    // ------------------------------------------------------------------------

    @Test
    public void testValidateValidAliases() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "user_id", DataTypes.INT()),
                        new DataField(1, "name", DataTypes.STRING()));
        Map<String, String> aliases = new HashMap<>();
        aliases.put("user_id", "userId");

        // Should not throw
        IcebergColumnAliasOptions.validateAliases(fields, aliases);
    }

    @Test
    public void testValidateNonExistentColumn() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "name", DataTypes.STRING()));
        Map<String, String> aliases = new HashMap<>();
        aliases.put("non_existent", "alias");

        assertThatThrownBy(() -> IcebergColumnAliasOptions.validateAliases(fields, aliases))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non_existent")
                .hasMessageContaining("does not exist");
    }

    @Test
    public void testValidateAliasConflictsWithColumnName() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "user_id", DataTypes.INT()),
                        new DataField(1, "name", DataTypes.STRING()));
        Map<String, String> aliases = new HashMap<>();
        aliases.put("user_id", "name"); // "name" already exists as a column

        assertThatThrownBy(() -> IcebergColumnAliasOptions.validateAliases(fields, aliases))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name")
                .hasMessageContaining("conflicts");
    }

    @Test
    public void testValidateAliasConflictsWithOtherAlias() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "col_a", DataTypes.INT()),
                        new DataField(1, "col_b", DataTypes.STRING()));
        Map<String, String> aliases = new HashMap<>();
        aliases.put("col_a", "same_alias");
        aliases.put("col_b", "same_alias"); // Duplicate alias

        assertThatThrownBy(() -> IcebergColumnAliasOptions.validateAliases(fields, aliases))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same_alias")
                .hasMessageContaining("duplicate");
    }

    @Test
    public void testValidateEmptyAlias() {
        List<DataField> fields = Collections.singletonList(new DataField(0, "id", DataTypes.INT()));
        Map<String, String> aliases = new HashMap<>();
        aliases.put("id", "");

        assertThatThrownBy(() -> IcebergColumnAliasOptions.validateAliases(fields, aliases))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");
    }

    @Test
    public void testValidateAliasWithInvalidCharacters() {
        List<DataField> fields = Collections.singletonList(new DataField(0, "id", DataTypes.INT()));
        Map<String, String> aliases = new HashMap<>();
        aliases.put("id", "invalid-alias"); // Hyphens not allowed in Iceberg identifiers

        assertThatThrownBy(() -> IcebergColumnAliasOptions.validateAliases(fields, aliases))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a valid identifier");
    }

    @Test
    public void testValidateAliasStartingWithDigit() {
        List<DataField> fields = Collections.singletonList(new DataField(0, "id", DataTypes.INT()));
        Map<String, String> aliases = new HashMap<>();
        aliases.put("id", "123alias");

        assertThatThrownBy(() -> IcebergColumnAliasOptions.validateAliases(fields, aliases))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a valid identifier");
    }

    @Test
    public void testValidateAliasWithUnderscore() {
        // Underscores should be valid
        List<DataField> fields = Collections.singletonList(new DataField(0, "id", DataTypes.INT()));
        Map<String, String> aliases = new HashMap<>();
        aliases.put("id", "valid_alias_name");

        // Should not throw
        IcebergColumnAliasOptions.validateAliases(fields, aliases);
    }

    @Test
    public void testValidateAliasSameAsOriginalColumn() {
        // Alias that equals the column's own name should be valid (no-op)
        List<DataField> fields = Collections.singletonList(new DataField(0, "id", DataTypes.INT()));
        Map<String, String> aliases = new HashMap<>();
        aliases.put("id", "id");

        // Should not throw - it's redundant but valid
        IcebergColumnAliasOptions.validateAliases(fields, aliases);
    }
}
