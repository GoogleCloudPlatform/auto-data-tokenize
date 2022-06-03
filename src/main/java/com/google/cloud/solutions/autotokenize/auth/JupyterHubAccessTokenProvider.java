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

    private final String gcsTokenKey;
    private static final Logger LOG = Logger.getLogger(JupyterHubAccessTokenProvider.class.getName());

    private final AuthProviderClient client;

    public JupyterHubAccessTokenProvider(String authProviderURL, String gcsTokenKey) {
        this.client = new AuthProviderClient(authProviderURL);
        this.gcsTokenKey = gcsTokenKey;
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
