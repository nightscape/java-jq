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

/**
 * Functional interface for receiving streaming jq output values.
 *
 * <p>Each output value from the jq filter is passed to this callback as it is produced,
 * allowing for immediate processing without buffering all results in memory.</p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * try (JqFilter filter = JqFilter.compile(library, ".[] | select(.active)")) {
 *     filter.process(jsonInput, output -> {
 *         System.out.println(output);
 *     });
 * }
 * }</pre>
 */
@FunctionalInterface
public interface JqOutputCallback {
    /**
     * Called for each output value produced by the jq filter.
     *
     * @param output the serialized JSON output value
     */
    void onOutput(String output);
}
