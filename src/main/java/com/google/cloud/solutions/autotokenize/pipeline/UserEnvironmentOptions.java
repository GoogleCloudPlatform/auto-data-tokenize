/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.solutions.autotokenize.pipeline;


import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Additional user specific options.
 */
public interface UserEnvironmentOptions extends GcpOptions {

    @Default.InstanceFactory(UserNameFactory.class)
    String getUserName();

    void setUserName(String userName);

    /**
     * Factory for generating username by looking up the 'user.name' system property.
     */
    class UserNameFactory implements DefaultValueFactory<String> {
        @Override
        public String create(PipelineOptions options) {
            return MoreObjects.firstNonNull(System.getProperty("user.name"), "");
        }
    }
    /**
     * Returns a normalized job name constructed from {@link ApplicationNameOptions#getAppName()},
     * {@link UserEnvironmentOptions#getUserName()}, the current time, and a random integer.
     * <p>
     * This implementation is almost identical as {@link org.apache.beam.sdk.options.PipelineOptions.JobNameFactory},
     * except that the username can be set from the pipeline options.
     *
     * <p>The normalization makes sure that the name matches the pattern of
     * [a-z]([-a-z0-9]*[a-z0-9])?.
     */
    class JobNameFactory implements DefaultValueFactory<String> {
        private static final DateTimeFormatter FORMATTER =
                DateTimeFormat.forPattern("MMddHHmmss").withZone(DateTimeZone.UTC);

        @Override
        public String create(PipelineOptions options) {
            UserEnvironmentOptions userEnvironmentOptions = options.as(UserEnvironmentOptions.class);
            String appName = options.as(ApplicationNameOptions.class).getAppName();
            String normalizedAppName =
                    appName == null || appName.length() == 0
                            ? "BeamApp"
                            : appName.toLowerCase().replaceAll("[^a-z0-9]", "0").replaceAll("^[^a-z]", "a");
            String userName = userEnvironmentOptions.getUserName();
            String normalizedUserName = userName.toLowerCase().replaceAll("[^a-z0-9]", "0");
            String datePart = FORMATTER.print(DateTimeUtils.currentTimeMillis());

            String randomPart = Integer.toHexString(ThreadLocalRandom.current().nextInt());
            return String.format(
                    "%s-%s-%s-%s", normalizedAppName, normalizedUserName, datePart, randomPart);
        }
    }    
}
