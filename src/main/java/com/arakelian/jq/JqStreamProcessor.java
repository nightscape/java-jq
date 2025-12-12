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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import com.google.common.base.Charsets;
import com.sun.jna.Memory;
import com.sun.jna.Pointer;

/**
 * A stream processor for feeding chunked JSON input to a jq filter.
 *
 * <p>This class allows processing very large JSON inputs incrementally,
 * without loading the entire input into memory at once. Input chunks are
 * fed to the parser, and output values are emitted via callback as they
 * become available.</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * try (JqFilter filter = JqFilter.compile(lib, ".[] | select(.active)")) {
 *     try (JqStreamProcessor processor = filter.createStreamProcessor(output -> {
 *         System.out.println(output);
 *     })) {
 *         // Feed chunks of JSON data
 *         processor.feedChunk("{\"items\": [", false);
 *         processor.feedChunk("{\"name\": \"a\", \"active\": true},", false);
 *         processor.feedChunk("{\"name\": \"b\", \"active\": false}", false);
 *         processor.feedChunk("]}", true); // finished=true on last chunk
 *     }
 * }
 * }</pre>
 *
 * <p><strong>Thread Safety:</strong> Operations on this processor are synchronized
 * with the global jq lock to ensure thread safety.</p>
 *
 * <p><strong>Important:</strong> You must call {@link #feedChunk(String, boolean)}
 * with {@code finished=true} at least once to signal end of input, or call
 * {@link #finish()} before closing.</p>
 */
public class JqStreamProcessor implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(JqStreamProcessor.class.getName());

    /**
     * Global lock for thread safety - shared with JqFilter and JqRequest.
     */
    private static final ReentrantLock SYNC = new ReentrantLock();

    private final JqFilter filter;
    private final JqOutputCallback callback;
    private final JqLibrary lib;
    private final Pointer parser;
    private final List<String> errors;

    private volatile boolean closed = false;
    private volatile boolean finished = false;

    /**
     * Creates a new stream processor.
     *
     * @param filter the compiled filter to use
     * @param callback called for each output value
     */
    JqStreamProcessor(JqFilter filter, JqOutputCallback callback) {
        this.filter = filter;
        this.callback = callback;
        this.lib = filter.getLib();
        this.errors = new ArrayList<>();

        SYNC.lock();
        try {
            LOGGER.log(FINE, "Creating parser for stream processing");
            this.parser = lib.jv_parser_new(0);

            // Set up error callback
            lib.jq_set_error_cb(filter.getJq(), (data, jv) -> {
                LOGGER.log(FINE, "Stream processing error callback");
                final int kind = lib.jv_get_kind(jv);
                if (kind == JqLibrary.JV_KIND_STRING) {
                    final String error = lib.jv_string_value(jv).replaceAll("\\s++$", "");
                    errors.add(error);
                }
            }, new Pointer(0));
        } finally {
            SYNC.unlock();
        }
    }

    /**
     * Feeds a chunk of JSON input to the processor.
     *
     * <p>Chunks can be any portion of valid JSON - they don't need to be
     * complete JSON values. The parser will buffer incomplete values until
     * more data arrives.</p>
     *
     * <p>When feeding the last chunk, set {@code finished} to {@code true}
     * to signal end of input. This allows the parser to finalize any
     * buffered partial values.</p>
     *
     * @param chunk the chunk of JSON text to process
     * @param finished true if this is the last chunk
     * @throws JqFilterException if a processing error occurs
     * @throws IllegalStateException if the processor is closed or already finished
     */
    public void feedChunk(String chunk, boolean finished) {
        ensureOpen();
        if (this.finished) {
            throw new IllegalStateException("Stream processing already finished");
        }

        SYNC.lock();
        try {
            byte[] bytes = chunk.getBytes(Charsets.UTF_8);
            LOGGER.log(FINE, "Feeding chunk to parser, finished=" + finished);
            if (bytes.length == 0) {
                lib.jv_parser_set_buf(parser, null, 0, finished);
            } else {
                Memory memory = new Memory(bytes.length);
                memory.write(0, bytes, 0, bytes.length);
                lib.jv_parser_set_buf(parser, memory, bytes.length, finished);
            }

            // Process any complete values that are now available
            filter.processParserOutput(parser, callback, errors);

            if (finished) {
                this.finished = true;
            }
        } finally {
            SYNC.unlock();
        }

        // Throw any accumulated errors
        if (!errors.isEmpty()) {
            throw new JqFilterException(errors.get(0), errors);
        }
    }

    /**
     * Feeds a chunk of JSON input as raw bytes.
     *
     * <p>This method is useful when working with byte streams or when
     * the input encoding is already known to be UTF-8.</p>
     *
     * @param bytes the chunk of UTF-8 encoded JSON
     * @param finished true if this is the last chunk
     * @throws JqFilterException if a processing error occurs
     * @throws IllegalStateException if the processor is closed or already finished
     */
    public void feedChunk(byte[] bytes, boolean finished) {
        ensureOpen();
        if (this.finished) {
            throw new IllegalStateException("Stream processing already finished");
        }

        SYNC.lock();
        try {
            LOGGER.log(FINE, "Feeding byte chunk to parser, finished=" + finished);
            if (bytes.length == 0) {
                lib.jv_parser_set_buf(parser, null, 0, finished);
            } else {
                Memory memory = new Memory(bytes.length);
                memory.write(0, bytes, 0, bytes.length);
                lib.jv_parser_set_buf(parser, memory, bytes.length, finished);
            }

            // Process any complete values that are now available
            filter.processParserOutput(parser, callback, errors);

            if (finished) {
                this.finished = true;
            }
        } finally {
            SYNC.unlock();
        }

        // Throw any accumulated errors
        if (!errors.isEmpty()) {
            throw new JqFilterException(errors.get(0), errors);
        }
    }

    /**
     * Signals that all input has been provided.
     *
     * <p>This is equivalent to calling {@code feedChunk("", true)} and is
     * useful when you've already fed all chunks but haven't signaled
     * end of input.</p>
     *
     * @throws JqFilterException if a processing error occurs
     */
    public void finish() {
        if (!finished) {
            feedChunk("", true);
        }
    }

    /**
     * Returns whether the processor has finished receiving input.
     *
     * @return true if finish() was called or feedChunk was called with finished=true
     */
    public boolean isFinished() {
        return finished;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("JqStreamProcessor has been closed");
        }
    }

    /**
     * Releases the stream processor resources.
     *
     * <p>If input processing wasn't finished, this will attempt to finish it first.</p>
     */
    @Override
    public void close() {
        if (!closed) {
            SYNC.lock();
            try {
                if (!closed) {
                    // Clear error callback
                    lib.jq_set_error_cb(filter.getJq(), null, null);

                    LOGGER.log(FINE, "Releasing stream parser");
                    lib.jv_parser_free(parser);
                    closed = true;
                    LOGGER.log(FINE, "Stream parser released");
                }
            } finally {
                SYNC.unlock();
            }
        }
    }
}
