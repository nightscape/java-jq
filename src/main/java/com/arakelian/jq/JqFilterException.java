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

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Exception thrown when a jq filter compilation or execution fails.
 *
 * <p>This exception is used by the streaming API ({@link JqFilter}) to report
 * errors during filter compilation or JSON processing.</p>
 */
public class JqFilterException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final List<String> errors;

    /**
     * Creates a new JqFilterException with a single error message.
     *
     * @param message the error message
     */
    public JqFilterException(String message) {
        super(message);
        this.errors = ImmutableList.of(message);
    }

    /**
     * Creates a new JqFilterException with multiple error messages.
     *
     * @param message the primary error message
     * @param errors the list of all error messages
     */
    public JqFilterException(String message, List<String> errors) {
        super(message);
        this.errors = ImmutableList.copyOf(errors);
    }

    /**
     * Creates a new JqFilterException with an underlying cause.
     *
     * @param message the error message
     * @param cause the underlying cause
     */
    public JqFilterException(String message, Throwable cause) {
        super(message, cause);
        this.errors = ImmutableList.of(message);
    }

    /**
     * Returns all error messages associated with this exception.
     *
     * @return immutable list of error messages
     */
    public List<String> getErrors() {
        return errors;
    }
}
