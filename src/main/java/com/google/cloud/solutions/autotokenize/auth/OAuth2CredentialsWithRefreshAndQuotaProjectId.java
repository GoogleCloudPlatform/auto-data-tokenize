package com.google.cloud.solutions.autotokenize.auth;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.OAuth2CredentialsWithRefresh;
import com.google.auth.oauth2.QuotaProjectIdProvider;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OAuth2CredentialsWithRefreshAndQuotaProjectId extends OAuth2CredentialsWithRefresh
        implements QuotaProjectIdProvider {

    private static final String QUOTA_PROJECT_ID_HEADER_KEY = "x-goog-user-project";

    private final String quotaProjectId;
    protected OAuth2CredentialsWithRefreshAndQuotaProjectId(AccessToken accessToken, OAuth2RefreshHandler refreshHandler,
                                                         String quotaProjectId) {
        super(accessToken, refreshHandler);
        this.quotaProjectId = quotaProjectId;
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

    public static class Builder extends OAuth2Credentials.Builder {

        private OAuth2RefreshHandler refreshHandler;
        private String quotaProjectId;

        public Builder() {
            super();
        }

        /**
         * Sets the {@link AccessToken} to be consumed. It must contain an expiration time otherwise an
         * {@link IllegalArgumentException} will be thrown.
         */
        @Override
        public OAuth2CredentialsWithRefreshAndQuotaProjectId.Builder setAccessToken(AccessToken token) {
            super.setAccessToken(token);
            return this;
        }

        /** Sets the {@link OAuth2RefreshHandler} to be used for token refreshes. */
        public OAuth2CredentialsWithRefreshAndQuotaProjectId.Builder setRefreshHandler(OAuth2RefreshHandler handler) {
            this.refreshHandler = handler;
            return this;
        }

        public OAuth2CredentialsWithRefreshAndQuotaProjectId.Builder setQuotaProjectId(String quotaProjectId) {
            this.quotaProjectId = quotaProjectId;
            return this;
        }

        public OAuth2CredentialsWithRefreshAndQuotaProjectId build() {
            return new OAuth2CredentialsWithRefreshAndQuotaProjectId(getAccessToken(), refreshHandler, quotaProjectId);
        }
    }
}
