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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IcebergColumnAliasOptions}. */
public class IcebergColumnAliasOptionsTest {

    private static final List<DataField> TWO_FIELDS =
            Arrays.asList(
                    new DataField(0, "user_id", DataTypes.INT()),
                    new DataField(1, "name", DataTypes.STRING()));

    @Test
    public void testParseAliases() {
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "4");
        options.put("metadata.iceberg.column.alias.user_id", "userId");
        options.put("metadata.iceberg.storage", "table-location");

        Map<String, String> aliases = IcebergColumnAliasOptions.parseAliases(options);

        assertThat(aliases).containsOnly(Map.entry("user_id", "userId"));
    }

    @Test
    public void testBuildNameMapping() {
        Map<String, String> aliases = Collections.singletonMap("user_id", "userId");

        String nameMapping = IcebergColumnAliasOptions.buildNameMapping(TWO_FIELDS, aliases);

        assertThat(nameMapping)
                .contains("\"names\":[\"user_id\",\"userId\"]")
                .contains("\"names\":[\"name\"]");
    }

    @Test
    public void testValidateNonExistentColumn() {
        Map<String, String> aliases = Collections.singletonMap("missing", "alias");

        assertThatThrownBy(() -> IcebergColumnAliasOptions.validateAliases(TWO_FIELDS, aliases))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    public void testValidateAliasConflictsWithColumnName() {
        Map<String, String> aliases = Collections.singletonMap("user_id", "name");

        assertThatThrownBy(() -> IcebergColumnAliasOptions.validateAliases(TWO_FIELDS, aliases))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("conflicts");
    }

    @Test
    public void testValidateDuplicateAliases() {
        Map<String, String> aliases = new HashMap<>();
        aliases.put("user_id", "same");
        aliases.put("name", "same");

        assertThatThrownBy(() -> IcebergColumnAliasOptions.validateAliases(TWO_FIELDS, aliases))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "invalid-alias", "123alias"})
    public void testValidateInvalidAliasFormat(String invalidAlias) {
        List<DataField> fields = Collections.singletonList(new DataField(0, "id", DataTypes.INT()));
        Map<String, String> aliases = Collections.singletonMap("id", invalidAlias);

        assertThatThrownBy(() -> IcebergColumnAliasOptions.validateAliases(fields, aliases))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"userId", "valid_alias", "id"})
    public void testValidateValidAliasFormat(String validAlias) {
        List<DataField> fields = Collections.singletonList(new DataField(0, "id", DataTypes.INT()));
        Map<String, String> aliases = Collections.singletonMap("id", validAlias);

        IcebergColumnAliasOptions.validateAliases(fields, aliases);
    }
}
