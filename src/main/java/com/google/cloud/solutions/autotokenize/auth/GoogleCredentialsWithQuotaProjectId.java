package com.google.cloud.solutions.autotokenize.auth;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.QuotaProjectIdProvider;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GoogleCredentialsWithQuotaProjectId extends GoogleCredentials implements QuotaProjectIdProvider {

    private static final String QUOTA_PROJECT_ID_HEADER_KEY = "x-goog-user-project";

    private final String quotaProjectId;

    public GoogleCredentialsWithQuotaProjectId(OAuth2Credentials credentials) {
        super(credentials.getAccessToken());
        if (credentials instanceof QuotaProjectIdProvider) {
            this.quotaProjectId = ((QuotaProjectIdProvider) credentials).getQuotaProjectId();
        } else {
            quotaProjectId = null;
        }
    }
    @Override
    public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
        Map<String, List<String>> newRequestMetadata = new HashMap<>(super.getRequestMetadata(uri));
        if (quotaProjectId != null) {
            newRequestMetadata.put(
                    QUOTA_PROJECT_ID_HEADER_KEY, Collections.singletonList(quotaProjectId));
        }
        return Collections.unmodifiableMap(newRequestMetadata);
    }

    @Override
    public String getQuotaProjectId() {
        return quotaProjectId;
    }
}