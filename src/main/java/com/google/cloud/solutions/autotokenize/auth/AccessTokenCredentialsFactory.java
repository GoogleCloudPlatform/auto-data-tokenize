package com.google.cloud.solutions.autotokenize.auth;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.common.flogger.GoogleLogger;
import org.apache.beam.sdk.extensions.gcp.auth.CredentialFactory;
import org.apache.beam.sdk.extensions.gcp.auth.GcpCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Date;

/** Construct an oauth credential based on access tokens */
public class AccessTokenCredentialsFactory implements CredentialFactory {

    public static final String AUTH_PROVIDER_URL_KEY = "LOCAL_USER_PATH";
    public static final String GCS_TOKEN_PROVIDER_KEY = "GCS_TOKEN_PROVIDER_KEY";
    public static final String GCS_TOKEN_KEY = "GCS_TOKEN";
    private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
    private final String quotaProjectId;

    public static CredentialFactory fromOptions(PipelineOptions options) {
        if (System.getenv().containsKey(AUTH_PROVIDER_URL_KEY) || System.getenv().containsKey(GCS_TOKEN_KEY)) {
            return new AccessTokenCredentialsFactory(options.as(GcpOptions.class).getProject());
        } else {
            return GcpCredentialFactory.fromOptions(options);
        }
    }

    public AccessTokenCredentialsFactory(String quotaProjectId) {
        this.quotaProjectId = quotaProjectId;
    }

    @Override
    public @Nullable Credentials getCredential() {
        if (System.getenv().containsKey(AUTH_PROVIDER_URL_KEY)) {
            logger.atInfo().log("Using credential provider url");
            return new OAuth2CredentialsWithRefreshAndQuotaProjectId.Builder()
                    .setQuotaProjectId(this.quotaProjectId)
                    .setAccessToken(new AccessToken("", new Date(0L)))
                    .setRefreshHandler(new JupyterHubAccessTokenProvider(System.getenv(AUTH_PROVIDER_URL_KEY),
                            System.getenv(GCS_TOKEN_PROVIDER_KEY))).build();
        } else if (System.getenv().containsKey(GCS_TOKEN_KEY)) {
            logger.atInfo().log("Using credentials from env variable");
            AccessToken accessToken = new AccessToken(System.getenv().get(GCS_TOKEN_KEY), new Date(System.currentTimeMillis()
                    + 3600 * 1000));
            return new OAuth2CredentialsWithRefreshAndQuotaProjectId.Builder()
                    .setQuotaProjectId(this.quotaProjectId)
                    .setAccessToken(new AccessToken("", new Date(0L)))
                    .setRefreshHandler(() -> accessToken).build();
        } else {
            // Pipelines that only access to public data should be able to run without credentials.
            return null;
        }
    }
}
