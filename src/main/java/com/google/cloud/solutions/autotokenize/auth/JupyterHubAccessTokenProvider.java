package com.google.cloud.solutions.autotokenize.auth;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2CredentialsWithRefresh;
import com.google.cloud.solutions.autotokenize.auth.client.AuthProviderClient;
import com.google.cloud.solutions.autotokenize.auth.client.AuthResponse;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Date;
import java.util.logging.Logger;

public class JupyterHubAccessTokenProvider implements OAuth2CredentialsWithRefresh.OAuth2RefreshHandler {

    public static final String AUTH_PROVIDER_URL_KEY = "LOCAL_USER_PATH";
    public static final String GCS_TOKEN_KEY = "GCS_TOKEN_PROVIDER_KEY";
    private final String gcsTokenKey;
    private static final Logger LOG = Logger.getLogger(JupyterHubAccessTokenProvider.class.getName());

    private final AuthProviderClient client = new AuthProviderClient(System.getenv().get(AUTH_PROVIDER_URL_KEY));

    public JupyterHubAccessTokenProvider() {
        this.gcsTokenKey = System.getenv().get(GCS_TOKEN_KEY);
        LOG.info(String.format("Ready to fetch '%s' tokens", this.gcsTokenKey));
    }

    @Override
    public AccessToken refreshAccessToken() throws IOException {
        AuthResponse response = client.getAuth();
        if (response.getExchangedTokens().isEmpty()) {
            throw new IllegalStateException("Exchange tokens missing. Can not retrieve access tokens for GCS");
        } else {
            AuthResponse.ExchangedToken token = response.getExchangedTokens().get(this.gcsTokenKey);
            return new AccessToken(token.getAccessToken(), new Date(token.getExpirationTimeMilliSeconds()));
        }
    }
}
