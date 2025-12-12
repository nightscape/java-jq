/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.arakelian.jq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.arakelian.jq.JqRequest.Indent;

/**
 * Tests for the streaming API (JqFilter, JqStreamProcessor).
 */
public class JqFilterTest {
    private static JqLibrary library;
    private static File modulePath;

    @BeforeAll
    public static void setup() {
        library = ImmutableJqLibrary.of();

        // Find module path for tests that need imports
        URL a_jq = JqFilterTest.class.getClassLoader().getResource("modules/a.jq");
        if (a_jq != null) {
            modulePath = new File(a_jq.getFile()).getParentFile();
        }
    }

    @Test
    public void testSimpleFilter() {
        try (JqFilter filter = JqFilter.compile(library, ".foo")) {
            List<String> results = new ArrayList<>();
            filter.process("{\"foo\": \"bar\"}", results::add);

            assertEquals(1, results.size());
            assertEquals("\"bar\"", results.get(0));
        }
    }

    @Test
    public void testIdentityFilter() {
        try (JqFilter filter = JqFilter.compile(library, ".")) {
            List<String> results = filter.processAll("{\"a\": 1}");

            assertEquals(1, results.size());
            assertEquals("{\"a\":1}", results.get(0));
        }
    }

    @Test
    public void testArrayIteration() {
        try (JqFilter filter = JqFilter.compile(library, ".[]")) {
            List<String> results = new ArrayList<>();
            filter.process("[1, 2, 3]", results::add);

            assertEquals(3, results.size());
            assertEquals("1", results.get(0));
            assertEquals("2", results.get(1));
            assertEquals("3", results.get(2));
        }
    }

    @Test
    public void testSelectFilter() {
        try (JqFilter filter = JqFilter.compile(library, ".[] | select(.active)")) {
            List<String> results = filter.processAll(
                    "[{\"name\": \"a\", \"active\": true}, {\"name\": \"b\", \"active\": false}, {\"name\": \"c\", \"active\": true}]");

            assertEquals(2, results.size());
            assertTrue(results.get(0).contains("\"name\":\"a\""));
            assertTrue(results.get(1).contains("\"name\":\"c\""));
        }
    }

    @Test
    public void testIteratorProcessing() {
        try (JqFilter filter = JqFilter.compile(library, ".[]")) {
            Iterator<String> iter = filter.processIterator("[\"a\", \"b\", \"c\"]");

            assertTrue(iter.hasNext());
            assertEquals("\"a\"", iter.next());
            assertTrue(iter.hasNext());
            assertEquals("\"b\"", iter.next());
            assertTrue(iter.hasNext());
            assertEquals("\"c\"", iter.next());
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void testFilterReuse() {
        try (JqFilter filter = JqFilter.compile(library, ".value")) {
            // Process multiple inputs with the same filter
            List<String> results1 = filter.processAll("{\"value\": 1}");
            List<String> results2 = filter.processAll("{\"value\": 2}");
            List<String> results3 = filter.processAll("{\"value\": 3}");

            assertEquals(1, results1.size());
            assertEquals("1", results1.get(0));

            assertEquals(1, results2.size());
            assertEquals("2", results2.get(0));

            assertEquals(1, results3.size());
            assertEquals("3", results3.get(0));
        }
    }

    @Test
    public void testFilterReuseManyTimes() {
        try (JqFilter filter = JqFilter.compile(library, ". + 1")) {
            for (int i = 0; i < 100; i++) {
                List<String> results = filter.processAll(String.valueOf(i));
                assertEquals(1, results.size());
                assertEquals(String.valueOf(i + 1), results.get(0));
            }
        }
    }

    @Test
    public void testPrettyPrint() {
        try (JqFilter filter = JqFilter.builder()
                .lib(library)
                .filter(".")
                .pretty(true)
                .indent(Indent.TWO_SPACES)
                .build()) {
            List<String> results = filter.processAll("{\"a\": 1, \"b\": 2}");

            assertEquals(1, results.size());
            assertTrue(results.get(0).contains("\n"));
        }
    }

    @Test
    public void testSortKeys() {
        try (JqFilter filter = JqFilter.builder()
                .lib(library)
                .filter(".")
                .sortKeys(true)
                .build()) {
            List<String> results = filter.processAll("{\"z\": 1, \"a\": 2}");

            assertEquals(1, results.size());
            // Keys should be sorted alphabetically
            assertTrue(results.get(0).indexOf("\"a\"") < results.get(0).indexOf("\"z\""));
        }
    }

    @Test
    public void testCompilationError() {
        JqFilterException exception = assertThrows(JqFilterException.class, () -> {
            JqFilter.compile(library, "invalid[[[filter");
        });

        assertNotNull(exception.getMessage());
        assertFalse(exception.getErrors().isEmpty());
    }

    @Test
    public void testProcessingError() {
        try (JqFilter filter = JqFilter.compile(library, ".foo.bar")) {
            // Processing null doesn't throw - it just produces null output
            List<String> results = filter.processAll("null");
            assertEquals(1, results.size());
            assertEquals("null", results.get(0));
        }
    }

    @Test
    public void testInvalidJson() {
        try (JqFilter filter = JqFilter.compile(library, ".")) {
            assertThrows(JqFilterException.class, () -> {
                filter.processAll("not valid json");
            });
        }
    }

    @Test
    public void testClosedFilterThrows() {
        JqFilter filter = JqFilter.compile(library, ".");
        filter.close();

        assertThrows(IllegalStateException.class, () -> {
            filter.processAll("{}");
        });
    }

    @Test
    public void testEmptyInput() {
        try (JqFilter filter = JqFilter.compile(library, ".")) {
            // Empty string should not produce any output
            List<String> results = filter.processAll("");
            assertEquals(0, results.size());
        }
    }

    @Test
    public void testMultipleJsonValues() {
        try (JqFilter filter = JqFilter.compile(library, ".")) {
            // Multiple JSON values in sequence (NDJSON style)
            List<String> results = filter.processAll("{\"a\":1}\n{\"b\":2}\n{\"c\":3}");

            assertEquals(3, results.size());
            assertEquals("{\"a\":1}", results.get(0));
            assertEquals("{\"b\":2}", results.get(1));
            assertEquals("{\"c\":3}", results.get(2));
        }
    }

    @Test
    public void testComplexTransformation() {
        String filter = ".items[] | {name: .name, value: (.price * .quantity)}";
        try (JqFilter jqFilter = JqFilter.compile(library, filter)) {
            String input = "{\"items\": [{\"name\": \"apple\", \"price\": 1.5, \"quantity\": 3}, {\"name\": \"banana\", \"price\": 0.5, \"quantity\": 6}]}";

            List<String> results = jqFilter.processAll(input);

            assertEquals(2, results.size());
            assertTrue(results.get(0).contains("\"apple\""));
            assertTrue(results.get(0).contains("4.5"));
            assertTrue(results.get(1).contains("\"banana\""));
            assertTrue(results.get(1).contains("3"));
        }
    }

    // Stream Processor Tests

    @Test
    public void testStreamProcessorBasic() {
        try (JqFilter filter = JqFilter.compile(library, ".[]")) {
            List<String> results = new ArrayList<>();

            try (JqStreamProcessor processor = filter.createStreamProcessor(results::add)) {
                processor.feedChunk("[1, 2, 3]", true);
            }

            assertEquals(3, results.size());
            assertEquals("1", results.get(0));
            assertEquals("2", results.get(1));
            assertEquals("3", results.get(2));
        }
    }

    @Test
    public void testStreamProcessorChunked() {
        try (JqFilter filter = JqFilter.compile(library, ".items[]")) {
            List<String> results = new ArrayList<>();

            try (JqStreamProcessor processor = filter.createStreamProcessor(results::add)) {
                // Feed JSON in chunks
                processor.feedChunk("{\"items\":", false);
                processor.feedChunk(" [1, ", false);
                processor.feedChunk("2, 3", false);
                processor.feedChunk("]}", true);
            }

            assertEquals(3, results.size());
            assertEquals("1", results.get(0));
            assertEquals("2", results.get(1));
            assertEquals("3", results.get(2));
        }
    }

    @Test
    public void testStreamProcessorMultipleObjects() {
        try (JqFilter filter = JqFilter.compile(library, ".value")) {
            List<String> results = new ArrayList<>();

            try (JqStreamProcessor processor = filter.createStreamProcessor(results::add)) {
                // Feed multiple JSON objects (NDJSON style)
                processor.feedChunk("{\"value\": 1}\n", false);
                processor.feedChunk("{\"value\": 2}\n", false);
                processor.feedChunk("{\"value\": 3}", true);
            }

            assertEquals(3, results.size());
            assertEquals("1", results.get(0));
            assertEquals("2", results.get(1));
            assertEquals("3", results.get(2));
        }
    }

    @Test
    public void testStreamProcessorWithBytes() {
        try (JqFilter filter = JqFilter.compile(library, ".")) {
            List<String> results = new ArrayList<>();

            try (JqStreamProcessor processor = filter.createStreamProcessor(results::add)) {
                processor.feedChunk("{\"test\":".getBytes(), false);
                processor.feedChunk("\"value\"}".getBytes(), true);
            }

            assertEquals(1, results.size());
            assertEquals("{\"test\":\"value\"}", results.get(0));
        }
    }

    @Test
    public void testStreamProcessorFinish() {
        try (JqFilter filter = JqFilter.compile(library, ".")) {
            List<String> results = new ArrayList<>();

            try (JqStreamProcessor processor = filter.createStreamProcessor(results::add)) {
                assertFalse(processor.isFinished());
                processor.feedChunk("{\"a\":1}", false);
                assertFalse(processor.isFinished());
                processor.finish();
                assertTrue(processor.isFinished());
            }

            assertEquals(1, results.size());
        }
    }

    @Test
    public void testStreamProcessorDoubleFinishThrows() {
        try (JqFilter filter = JqFilter.compile(library, ".")) {
            try (JqStreamProcessor processor = filter.createStreamProcessor(s -> {})) {
                processor.feedChunk("{}", true);

                assertThrows(IllegalStateException.class, () -> {
                    processor.feedChunk("{}", false);
                });
            }
        }
    }

    @Test
    public void testStreamProcessorClosedThrows() {
        try (JqFilter filter = JqFilter.compile(library, ".")) {
            JqStreamProcessor processor = filter.createStreamProcessor(s -> {});
            processor.close();

            assertThrows(IllegalStateException.class, () -> {
                processor.feedChunk("{}", true);
            });
        }
    }

    @Test
    public void testStreamProcessorInvalidJson() {
        try (JqFilter filter = JqFilter.compile(library, ".")) {
            try (JqStreamProcessor processor = filter.createStreamProcessor(s -> {})) {
                assertThrows(JqFilterException.class, () -> {
                    processor.feedChunk("invalid json {{{", true);
                });
            }
        }
    }

    @Test
    public void testLargeArrayStreaming() {
        // Test with a larger array to verify streaming works
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < 1000; i++) {
            if (i > 0) sb.append(",");
            sb.append(i);
        }
        sb.append("]");

        try (JqFilter filter = JqFilter.compile(library, ".[]")) {
            List<String> results = new ArrayList<>();
            filter.process(sb.toString(), results::add);

            assertEquals(1000, results.size());
            for (int i = 0; i < 1000; i++) {
                assertEquals(String.valueOf(i), results.get(i));
            }
        }
    }

    @Test
    public void testModulePath() {
        if (modulePath == null) {
            return; // Skip if module path not available
        }

        try (JqFilter filter = JqFilter.builder()
                .lib(library)
                .filter("import \"a\" as a; a::a")
                .addModulePath(modulePath)
                .build()) {
            List<String> results = filter.processAll("null");

            assertEquals(1, results.size());
            assertEquals("\"a\"", results.get(0));
        }
    }

    @Test
    public void testBuilderDefaults() {
        // Test that builder works with minimal configuration
        try (JqFilter filter = JqFilter.builder()
                .lib(library)
                .build()) {
            // Default filter is "."
            assertEquals(".", filter.getFilter());

            List<String> results = filter.processAll("{\"x\":1}");
            assertEquals(1, results.size());
            assertEquals("{\"x\":1}", results.get(0));
        }
    }

    @Test
    public void testCallbackOrder() {
        try (JqFilter filter = JqFilter.compile(library, ".[]")) {
            List<Integer> order = new ArrayList<>();
            int[] counter = {0};

            filter.process("[1, 2, 3, 4, 5]", output -> {
                order.add(++counter[0]);
            });

            // Callbacks should be called in order
            assertEquals(5, order.size());
            for (int i = 0; i < 5; i++) {
                assertEquals(i + 1, order.get(i));
            }
        }
    }
}
