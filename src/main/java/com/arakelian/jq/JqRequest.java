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

import java.io.File;
import java.util.List;
import java.util.Map;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableMap;

@Value.Immutable
public abstract class JqRequest {
    public enum Indent {
        NONE, //
        TAB, //
        SPACE, //
        TWO_SPACES;
    }

    public final JqResponse execute() {
        try (JqFilter filter = JqFilter.builder()
                .lib(getLib())
                .filter(getFilter())
                .pretty(isPretty())
                .indent(getIndent())
                .sortKeys(isSortKeys())
                .modulePaths(getModulePaths())
                .argJson(getArgJson())
                .build()) {

            List<String> outputs = filter.processAll(getInput());
            return ImmutableJqResponse.builder()
                    .output(String.join(getStreamSeparator(), outputs))
                    .build();
        } catch (JqFilterException e) {
            return ImmutableJqResponse.builder()
                    .addAllErrors(e.getErrors())
                    .build();
        }
    }

    @Value.Default
    public Map<String, String> getArgJson() {
        return ImmutableMap.of();
    }

    @Value.Default
    public String getFilter() {
        return ".";
    }

    @Value.Default
    public Indent getIndent() {
        return Indent.TWO_SPACES;
    }

    public abstract String getInput();

    public abstract JqLibrary getLib();

    public abstract List<File> getModulePaths();

    @Value.Default
    public String getStreamSeparator() {
        return "\n";
    }

    @Value.Default
    public boolean isPretty() {
        return true;
    }

    @Value.Default
    public boolean isSortKeys() {
        return false;
    }
}
