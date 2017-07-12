/**
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
package org.apache.camel.component.google.pubsub;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import com.google.api.client.util.Strings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.spi.v1.TopicAdminSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GooglePubsubCredentialsProviderBuilder {
    private String serviceAccount;
    private String serviceAccountKey;
    private String credentialsFileLocation;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public CredentialsProvider build() throws Exception {
        CredentialsProvider credentialsProvider = null;

        if (!Strings.isNullOrEmpty(serviceAccount) && !Strings.isNullOrEmpty(serviceAccountKey)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Service Account and Key have been set explicitly. Initialising PubSub using Service Account " + serviceAccount);
            }
            credentialsProvider = FixedCredentialsProvider.create(createFromAccountKeyPair());
        }

        if (credentialsProvider == null && !Strings.isNullOrEmpty(credentialsFileLocation)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Key File Name has been set explicitly. Initialising PubSub using Key File " + credentialsFileLocation);
            }
            credentialsProvider = FixedCredentialsProvider.create(createFromFile());
        }

        if (credentialsProvider == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("No explicit Service Account or Key File Name have been provided. Initialising PubSub using defaults ");
            }
            credentialsProvider = GoogleCredentialsProvider.newBuilder().setScopesToApply(TopicAdminSettings.getDefaultServiceScopes()).build();
        }

        return credentialsProvider;

    }

    private GoogleCredentials createFromFile() throws Exception {

        GoogleCredentials credential = GoogleCredentials.fromStream(new FileInputStream(credentialsFileLocation));

        if (credential.createScopedRequired()) {
            credential = credential.createScoped(TopicAdminSettings.getDefaultServiceScopes());
        }

        return credential;
    }

    private GoogleCredentials createFromAccountKeyPair() throws IOException {
        return ServiceAccountCredentials.fromPkcs8(
                null,
                serviceAccount,
                serviceAccountKey,
                null,
                TopicAdminSettings.getDefaultServiceScopes()
        );
    }

    public String getServiceAccount() {
        return serviceAccount;
    }

    public GooglePubsubCredentialsProviderBuilder setServiceAccount(String serviceAccount) {
        this.serviceAccount = serviceAccount;
        return this;
    }

    public String getServiceAccountKey() {
        return serviceAccountKey;
    }

    public GooglePubsubCredentialsProviderBuilder setServiceAccountKey(String serviceAccountKey) {
        this.serviceAccountKey = serviceAccountKey;
        return this;
    }

    public String getCredentialsFileLocation() {
        return credentialsFileLocation;
    }

    public GooglePubsubCredentialsProviderBuilder setCredentialsFileLocation(String credentialsFileLocation) {
        this.credentialsFileLocation = credentialsFileLocation;
        return this;
    }
}
