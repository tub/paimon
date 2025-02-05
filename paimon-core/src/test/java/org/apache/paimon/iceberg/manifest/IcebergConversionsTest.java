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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IcebergConversionsTest {

    @Test
    @DisplayName("Test empty string conversion")
    void testEmptyString() {
        String empty = "";
        ByteBuffer result = IcebergConversions.toByteBuffer(DataTypes.VARCHAR(10), empty);
        String decodedString = new String(result.array(), StandardCharsets.UTF_8);
        assertThat(result.array()).isEmpty();
        assertThat(empty).isEqualTo(decodedString);
    }

    @Test
    @DisplayName("Test null handling")
    void testNullHandling() {
        assertThatThrownBy(() -> IcebergConversions.toByteBuffer(DataTypes.VARCHAR(10), null))
                .isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @MethodSource("provideSpecialStrings")
    @DisplayName("Test special string cases")
    void testSpecialStrings(String input) {
        ByteBuffer result = IcebergConversions.toByteBuffer(DataTypes.VARCHAR(100), input);
        String decoded = new String(result.array(), 0, result.limit(), StandardCharsets.UTF_8);
        assertThat(decoded).isEqualTo(input);
    }

    private static Stream<Arguments> provideSpecialStrings() {
        return Stream.of(
                Arguments.of("Hello\u0000World"), // Embedded null
                Arguments.of("\n\r\t"), // Control characters
                Arguments.of(" "), // Single space
                Arguments.of("    "), // Multiple spaces
                Arguments.of("①②③"), // Unicode numbers
                Arguments.of("🌟🌞🌝"), // Emojis
                Arguments.of("Hello\uD83D\uDE00World"), // Surrogate pairs
                Arguments.of("\uFEFF"), // Byte Order Mark
                Arguments.of("Hello\\World"), // Backslashes
                Arguments.of("Hello\"World"), // Quotes
                Arguments.of("Hello'World"), // Single quotes
                Arguments.of("Hello\bWorld"), // Backspace
                Arguments.of("Hello\fWorld") // Form feed
                );
    }

    @ParameterizedTest
    @MethodSource("provideLongStrings")
    @DisplayName("Test various length strings")
    void testLongStrings(String input) {
        ByteBuffer result =
                IcebergConversions.toByteBuffer(DataTypes.VARCHAR(input.length()), input);
        String decoded = new String(result.array(), 0, result.limit(), StandardCharsets.UTF_8);
        assertThat(decoded).isEqualTo(input).hasSize(input.length());
    }

    private static Stream<Arguments> provideLongStrings() {
        return Stream.of(
                Arguments.of(createString(1)),
                Arguments.of(createString(10)),
                Arguments.of(createString(100)),
                Arguments.of(createString(1000)),
                Arguments.of(createString(10000)));
    }

    private static String createString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

    @Test
    @DisplayName("Test multi-byte UTF-8 characters")
    void testMultiByteCharacters() {
        String[] inputs = {
            "中文", // Chinese
            "한글", // Korean
            "日本語", // Japanese
            "🌟", // Emoji (4 bytes)
            "Café", // Latin-1 Supplement
            "Привет", // Cyrillic
            "שָׁלוֹם", // Hebrew with combining marks
            "ᄀᄁᄂᄃᄄ", // Hangul Jamo
            "बहुत बढ़िया", // Devanagari
            "العربية" // Arabic
        };

        for (String input : inputs) {
            ByteBuffer result =
                    IcebergConversions.toByteBuffer(DataTypes.VARCHAR(input.length() * 4), input);
            String decoded = new String(result.array(), 0, result.limit(), StandardCharsets.UTF_8);
            assertThat(decoded).isEqualTo(input);
            assertThat(result.limit()).isGreaterThanOrEqualTo(input.length());
        }
    }

    @Test
    @DisplayName("Test buffer capacity and limits")
    void testBufferProperties() {
        String input = "Hello, World!";
        ByteBuffer result =
                IcebergConversions.toByteBuffer(DataTypes.VARCHAR(input.length()), input);

        assertThat(result.limit()).isEqualTo(result.array().length);
        assertThat(containsTrailingZeros(result)).isFalse();
    }

    @Test
    @DisplayName("Test concurrent access")
    void testConcurrentAccess() throws InterruptedException {
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];
        String[] inputs = new String[threadCount];
        ByteBuffer[] results = new ByteBuffer[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            inputs[index] = "Thread" + index;
            threads[index] =
                    new Thread(
                            () -> {
                                results[index] =
                                        IcebergConversions.toByteBuffer(
                                                DataTypes.VARCHAR(inputs[index].length()),
                                                inputs[index]);
                            });
            threads[index].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        for (int i = 0; i < threadCount; i++) {
            String decoded =
                    new String(results[i].array(), 0, results[i].limit(), StandardCharsets.UTF_8);
            assertThat(decoded).isEqualTo(inputs[i]);
        }
    }

    private boolean containsTrailingZeros(ByteBuffer buffer) {
        byte[] array = buffer.array();
        for (int i = buffer.limit(); i < array.length; i++) {
            if (array[i] != 0) {
                return true;
            }
        }
        return false;
    }

    @ParameterizedTest
    @MethodSource("provideTimestampConversionCases")
    void testTimestampToByteBuffer(int precision, Timestamp input, long expectedValue) {
        ByteBuffer buffer = IcebergConversions.toByteBuffer(DataTypes.TIMESTAMP(precision), input);
        assertThat(buffer.order()).isEqualTo(ByteOrder.LITTLE_ENDIAN);
        assertThat(buffer.getLong(0)).isEqualTo(expectedValue);
    }

    private static Stream<Arguments> provideTimestampConversionCases() {
        Timestamp timestamp3 = Timestamp.fromEpochMillis(1682164983524L); // 2023-04-22T13:03:03.524
        Timestamp timestamp6 =
                Timestamp.fromMicros(1683849603123456L); // 2023-05-12 00:00:03.123456

        return Stream.of(
                Arguments.of(3, timestamp3, 1682164983524L),
                Arguments.of(6, timestamp3, 1682164983524000L),
                Arguments.of(6, timestamp6, 1683849603123456L));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidPrecisions")
    @DisplayName("Test invalid timestamp precisions for ByteBuffer conversion")
    void testTimestampToByteBufferInvalidPrecisions(int precision) {
        Timestamp timestamp = Timestamp.fromEpochMillis(1682164983524L);

        assertThatThrownBy(
                        () ->
                                IcebergConversions.toByteBuffer(
                                        DataTypes.TIMESTAMP(precision), timestamp))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Paimon Iceberg compatibility supports only milli/microsecond precision timestamps.");
    }

    private static Stream<Arguments> provideInvalidPrecisions() {
        return Stream.of(
                Arguments.of(0),
                Arguments.of(1),
                Arguments.of(2),
                Arguments.of(4),
                Arguments.of(5),
                Arguments.of(7),
                Arguments.of(9));
    }

    // ------------------------------------------------------------------------
    //  toPaimonObject tests
    // ------------------------------------------------------------------------

    @ParameterizedTest
    @MethodSource("provideTimestampTestCases")
    public void testToPaimonObjectForTimestamp(int precision, long sampleTs, String expectedTs) {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putLong(sampleTs);

        Timestamp actualTs =
                (Timestamp)
                        IcebergConversions.toPaimonObject(DataTypes.TIMESTAMP(precision), bytes);
        assertThat(actualTs.toString()).isEqualTo(expectedTs);
    }

    private static Stream<Arguments> provideTimestampTestCases() {
        return Stream.of(
                Arguments.of(3, -1356022717123L, "1927-01-12T07:01:22.877"),
                Arguments.of(3, 1713790983524L, "2024-04-22T13:03:03.524"),
                Arguments.of(6, 1640690931207203L, "2021-12-28T11:28:51.207203"));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidTimestampCases")
    void testToPaimonObjectTimestampInvalid(int precision, long sampleTs) {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putLong(sampleTs);

        assertThatThrownBy(
                        () ->
                                IcebergConversions.toPaimonObject(
                                        DataTypes.TIMESTAMP(precision), bytes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Paimon Iceberg compatibility supports only milli/microsecond precision timestamps.");
    }

    private static Stream<Arguments> provideInvalidTimestampCases() {
        return Stream.of(Arguments.of(0, 1698686153L), Arguments.of(9, 1698686153123456789L));
    }
}
