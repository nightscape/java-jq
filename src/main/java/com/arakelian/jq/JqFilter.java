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

import static java.util.logging.Level.FINE;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import com.arakelian.jq.JqLibrary.Jv;
import com.arakelian.jq.JqRequest.Indent;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.jna.Memory;
import com.sun.jna.Pointer;

/**
 * A compiled jq filter that can be reused for processing multiple inputs.
 *
 * <p>This class provides a streaming API for jq processing, allowing:</p>
 * <ul>
 *   <li>Compiling a filter once and reusing it across multiple inputs</li>
 *   <li>Receiving output values via callback as they are produced</li>
 *   <li>Processing large inputs via chunked streaming</li>
 *   <li>Iterator-based access to output values</li>
 * </ul>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * JqLibrary lib = ImmutableJqLibrary.of();
 * try (JqFilter filter = JqFilter.compile(lib, ".[] | select(.active)")) {
 *     // Callback-based processing
 *     filter.process(jsonInput, output -> System.out.println(output));
 *
 *     // Iterator-based processing
 *     Iterator<String> results = filter.processIterator(anotherInput);
 *     while (results.hasNext()) {
 *         System.out.println(results.next());
 *     }
 *
 *     // Chunked input processing
 *     try (JqStreamProcessor processor = filter.createStreamProcessor(output -> ...)) {
 *         processor.feedChunk(chunk1, false);
 *         processor.feedChunk(chunk2, true); // finished=true on last chunk
 *     }
 * }
 * }</pre>
 *
 * <p><strong>Thread Safety:</strong> jq is not thread-safe. All operations on this filter
 * are synchronized with a global lock to ensure thread safety.</p>
 */
public class JqFilter implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(JqFilter.class.getName());

    /**
     * Global lock for thread safety - jq is not thread-safe.
     * Shared with JqRequest to ensure consistency.
     */
    private static final ReentrantLock SYNC = new ReentrantLock();

    private final JqLibrary lib;
    private final String filter;
    private final Pointer jq;
    private final int dumpFlags;
    private final List<String> modulePaths;
    private final Map<String, String> argJson;
    private volatile boolean closed = false;

    /**
     * Private constructor - use {@link #compile(JqLibrary, String)} or builder methods.
     */
    private JqFilter(JqLibrary lib, String filter, Pointer jq, int dumpFlags,
                     List<String> modulePaths, Map<String, String> argJson) {
        this.lib = lib;
        this.filter = filter;
        this.jq = jq;
        this.dumpFlags = dumpFlags;
        this.modulePaths = ImmutableList.copyOf(modulePaths);
        this.argJson = ImmutableMap.copyOf(argJson);
    }

    /**
     * Compiles a jq filter expression for reuse.
     *
     * @param lib the jq library instance
     * @param filter the jq filter expression to compile
     * @return a compiled filter that can be reused
     * @throws JqFilterException if the filter cannot be compiled
     */
    public static JqFilter compile(JqLibrary lib, String filter) {
        return builder()
                .lib(lib)
                .filter(filter)
                .build();
    }

    /**
     * Creates a new builder for configuring a JqFilter.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for creating JqFilter instances with custom configuration.
     */
    public static class Builder {
        private JqLibrary lib;
        private String filter = ".";
        private boolean pretty = false;
        private Indent indent = Indent.NONE;
        private boolean sortKeys = false;
        private List<File> modulePaths = Collections.emptyList();
        private Map<String, String> argJson = Collections.emptyMap();

        /**
         * Sets the jq library instance (required).
         *
         * @param lib the jq library
         * @return this builder
         */
        public Builder lib(JqLibrary lib) {
            this.lib = lib;
            return this;
        }

        /**
         * Sets the jq filter expression (defaults to ".").
         *
         * @param filter the filter expression
         * @return this builder
         */
        public Builder filter(String filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Sets whether output should be pretty-printed (defaults to false).
         *
         * @param pretty true for pretty-printed output
         * @return this builder
         */
        public Builder pretty(boolean pretty) {
            this.pretty = pretty;
            return this;
        }

        /**
         * Sets the indentation style (defaults to NONE).
         *
         * @param indent the indentation style
         * @return this builder
         */
        public Builder indent(Indent indent) {
            this.indent = indent;
            return this;
        }

        /**
         * Sets whether to sort object keys in output (defaults to false).
         *
         * @param sortKeys true to sort keys
         * @return this builder
         */
        public Builder sortKeys(boolean sortKeys) {
            this.sortKeys = sortKeys;
            return this;
        }

        /**
         * Sets the module search paths for jq imports.
         *
         * @param modulePaths list of directories to search for modules
         * @return this builder
         */
        public Builder modulePaths(List<File> modulePaths) {
            this.modulePaths = modulePaths;
            return this;
        }

        /**
         * Adds a single module search path.
         *
         * @param modulePath directory to search for modules
         * @return this builder
         */
        public Builder addModulePath(File modulePath) {
            if (this.modulePaths.isEmpty()) {
                this.modulePaths = new ArrayList<>();
            }
            this.modulePaths.add(modulePath);
            return this;
        }

        /**
         * Sets the JSON arguments (equivalent to jq --argjson).
         *
         * @param argJson map of variable names to JSON values
         * @return this builder
         */
        public Builder argJson(Map<String, String> argJson) {
            this.argJson = argJson;
            return this;
        }

        /**
         * Builds and compiles the JqFilter.
         *
         * @return the compiled filter
         * @throws JqFilterException if the filter cannot be compiled
         */
        public JqFilter build() {
            Preconditions.checkNotNull(lib, "lib is required");
            Preconditions.checkNotNull(filter, "filter is required");

            int flags = 0;
            if (pretty) {
                flags |= JqLibrary.JV_PRINT_PRETTY;
            }
            switch (indent) {
                case TAB:
                    flags |= JqLibrary.JV_PRINT_TAB;
                    break;
                case SPACE:
                    flags |= JqLibrary.JV_PRINT_SPACE1;
                    break;
                case TWO_SPACES:
                    flags |= JqLibrary.JV_PRINT_SPACE2;
                    break;
                case NONE:
                default:
                    break;
            }
            if (sortKeys) {
                flags |= JqLibrary.JV_PRINT_SORTED;
            }

            List<String> modulePathStrings = new ArrayList<>();
            for (File file : modulePaths) {
                try {
                    modulePathStrings.add(file.getCanonicalPath());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            SYNC.lock();
            try {
                return compileInternal(lib, filter, flags, modulePathStrings, argJson);
            } finally {
                SYNC.unlock();
            }
        }
    }

    private static JqFilter compileInternal(JqLibrary lib, String filter, int dumpFlags,
                                            List<String> modulePaths, Map<String, String> argJson) {
        LOGGER.log(FINE, "Initializing JQ for filter compilation");
        final Pointer jq = lib.jq_init();
        Preconditions.checkState(jq != null, "jq_init returned null");

        // Set up module paths
        Jv moduleDirs = lib.jv_array();
        for (String dir : modulePaths) {
            LOGGER.log(FINE, "Using module path: " + dir);
            moduleDirs = lib.jv_array_append(moduleDirs, lib.jv_string(dir));
        }
        lib.jq_set_attr(jq, lib.jv_string("JQ_LIBRARY_PATH"), moduleDirs);

        // Collect compilation errors
        List<String> errors = new ArrayList<>();
        lib.jq_set_error_cb(jq, (data, jv) -> {
            LOGGER.log(FINE, "Compilation error callback");
            final int kind = lib.jv_get_kind(jv);
            if (kind == JqLibrary.JV_KIND_STRING) {
                final String error = lib.jv_string_value(jv).replaceAll("\\s++$", "");
                errors.add(error);
            }
        }, new Pointer(0));

        // Build arguments
        Jv args = lib.jv_object();
        for (String varname : argJson.keySet()) {
            String text = argJson.get(varname);
            Jv json = lib.jv_parse(text);
            if (!lib.jv_is_valid(json)) {
                lib.jq_set_error_cb(jq, null, null);
                lib.jq_teardown(jq);
                throw new JqFilterException("Invalid JSON text passed to --argjson (name: " + varname + ")");
            }
            args = lib.jv_object_set(args, lib.jv_string(varname), json);
        }

        // Compile the filter
        LOGGER.log(FINE, "Compiling filter: " + filter);
        if (!lib.jq_compile_args(jq, filter, lib.jv_copy(args))) {
            lib.jq_set_error_cb(jq, null, null);
            lib.jq_teardown(jq);
            String message = errors.isEmpty() ? "Filter compilation failed: " + filter
                    : errors.get(0);
            throw new JqFilterException(message, errors);
        }

        // Clear error callback after successful compilation
        lib.jq_set_error_cb(jq, null, null);

        LOGGER.log(FINE, "Filter compiled successfully");
        return new JqFilter(lib, filter, jq, dumpFlags, modulePaths, argJson);
    }

    /**
     * Processes JSON input and calls the callback for each output value.
     *
     * @param input the JSON input to process
     * @param callback called for each output value produced by the filter
     * @throws JqFilterException if processing fails
     */
    public void process(String input, JqOutputCallback callback) {
        ensureOpen();
        SYNC.lock();
        try {
            processInternal(input, callback);
        } finally {
            SYNC.unlock();
        }
    }

    /**
     * Processes JSON input and returns an iterator over output values.
     *
     * <p>Note: The iterator must be fully consumed before processing more input
     * with this filter, as jq maintains internal state during iteration.</p>
     *
     * @param input the JSON input to process
     * @return iterator over output values
     * @throws JqFilterException if processing fails
     */
    public Iterator<String> processIterator(String input) {
        ensureOpen();
        List<String> results = new ArrayList<>();
        SYNC.lock();
        try {
            processInternal(input, results::add);
        } finally {
            SYNC.unlock();
        }
        return results.iterator();
    }

    /**
     * Processes JSON input and returns all output values as a list.
     *
     * @param input the JSON input to process
     * @return list of all output values
     * @throws JqFilterException if processing fails
     */
    public List<String> processAll(String input) {
        ensureOpen();
        List<String> results = new ArrayList<>();
        SYNC.lock();
        try {
            processInternal(input, results::add);
        } finally {
            SYNC.unlock();
        }
        return results;
    }

    /**
     * Creates a stream processor for chunked input processing.
     *
     * <p>Use this when you need to process very large inputs that should be
     * fed incrementally rather than loaded entirely into memory.</p>
     *
     * <p>Example:</p>
     * <pre>{@code
     * try (JqStreamProcessor processor = filter.createStreamProcessor(output -> ...)) {
     *     processor.feedChunk(chunk1, false);
     *     processor.feedChunk(chunk2, false);
     *     processor.feedChunk(chunk3, true); // finished=true on last chunk
     * }
     * }</pre>
     *
     * @param callback called for each output value produced
     * @return a new stream processor
     */
    public JqStreamProcessor createStreamProcessor(JqOutputCallback callback) {
        ensureOpen();
        return new JqStreamProcessor(this, callback);
    }

    private void processInternal(String input, JqOutputCallback callback) {
        List<String> errors = new ArrayList<>();

        // Set up error callback for this processing session
        lib.jq_set_error_cb(jq, (data, jv) -> {
            LOGGER.log(FINE, "Processing error callback");
            final int kind = lib.jv_get_kind(jv);
            if (kind == JqLibrary.JV_KIND_STRING) {
                final String error = lib.jv_string_value(jv).replaceAll("\\s++$", "");
                errors.add(error);
            }
        }, new Pointer(0));

        try {
            // Create parser for this input
            Pointer parser = lib.jv_parser_new(0);
            try {
                byte[] inputBytes = input.getBytes(Charsets.UTF_8);
                if (inputBytes.length == 0) {
                    LOGGER.log(FINE, "Empty input, signaling end to parser");
                    lib.jv_parser_set_buf(parser, null, 0, true);
                } else {
                    Memory memory = new Memory(inputBytes.length);
                    memory.write(0, inputBytes, 0, inputBytes.length);

                    LOGGER.log(FINE, "Feeding input to parser");
                    lib.jv_parser_set_buf(parser, memory, inputBytes.length, true);
                }

                // Process all parsed values
                processParserOutput(parser, callback, errors);

            } finally {
                LOGGER.log(FINE, "Releasing parser");
                lib.jv_parser_free(parser);
            }
        } finally {
            lib.jq_set_error_cb(jq, null, null);
        }

        if (!errors.isEmpty()) {
            throw new JqFilterException(errors.get(0), errors);
        }
    }

    /**
     * Internal method to process output from a parser.
     * Used by both process() and JqStreamProcessor.
     */
    void processParserOutput(Pointer parser, JqOutputCallback callback, List<String> errors) {
        for (;;) {
            LOGGER.log(FINE, "Getting next parsed value");
            Jv parsed = lib.jv_parser_next(parser);

            if (!lib.jv_is_valid(parsed)) {
                // Check if this is an error or just end of input
                String message = getInvalidMessage(parsed);
                if (message != null) {
                    errors.add(message);
                }
                break;
            }

            // Process through filter
            LOGGER.log(FINE, "Starting jq processing");
            lib.jq_start(jq, parsed);

            for (;;) {
                Jv next = lib.jq_next(jq);
                if (!lib.jv_is_valid(next)) {
                    String message = getInvalidMessage(next);
                    if (message != null) {
                        errors.add(message);
                    }
                    break;
                }

                LOGGER.log(FINE, "Dumping output value");
                String output = lib.jv_dump_string(next, dumpFlags);
                callback.onOutput(output);
            }
        }
    }

    private String getInvalidMessage(Jv value) {
        Jv copy = lib.jv_copy(value);
        if (lib.jv_invalid_has_msg(copy)) {
            Jv message = lib.jv_invalid_get_msg(value);
            return lib.jv_string_value(message);
        } else {
            lib.jv_free(value);
            return null;
        }
    }

    /**
     * Returns the jq library instance.
     */
    JqLibrary getLib() {
        return lib;
    }

    /**
     * Returns the compiled jq state pointer.
     */
    Pointer getJq() {
        return jq;
    }

    /**
     * Returns the dump flags for serialization.
     */
    int getDumpFlags() {
        return dumpFlags;
    }

    /**
     * Returns the filter expression.
     *
     * @return the filter string
     */
    public String getFilter() {
        return filter;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("JqFilter has been closed");
        }
    }

    /**
     * Releases the compiled filter resources.
     *
     * <p>After closing, this filter cannot be used for processing.</p>
     */
    @Override
    public void close() {
        if (!closed) {
            SYNC.lock();
            try {
                if (!closed) {
                    LOGGER.log(FINE, "Releasing JQ state");
                    lib.jq_teardown(jq);
                    closed = true;
                    LOGGER.log(FINE, "JQ state released");
                }
            } finally {
                SYNC.unlock();
            }
        }
    }
}
