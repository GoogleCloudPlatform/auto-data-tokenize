package com.google.cloud.solutions.autotokenize.auth.client;

import com.google.gson.Gson;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AuthProviderClient {

    private static final Logger LOG = Logger.getLogger(AuthProviderClient.class.getName());
    private final String authProviderUrl;
    private OkHttpClient client;

    public AuthProviderClient(String authProviderUrl) {
        this.authProviderUrl = authProviderUrl;
        OkHttpClient.Builder builder = new OkHttpClient.Builder().callTimeout(20, TimeUnit.SECONDS);
        builder.addInterceptor(new BearerTokenInterceptor(new JHubTokenSupplier()));
        client = builder.build();
        LOG.info(String.format("Using auth provider url: %s", this.authProviderUrl));
    }

    public AuthResponse getAuth() {
        Request request = new Request.Builder()
                .url(this.authProviderUrl).get().build();

        try (Response response = client.newCall(request).execute()) {
            String json = getJson(response);
            handleErrorCodes(response, json);
            Gson gson = new Gson();
            return gson.fromJson(json, AuthResponse.class);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "getAccessToken failed", e);
            throw new AuthProviderException(e);
        }
    }

    private String getJson(Response response) throws IOException {
        ResponseBody body = response.body();
        if (body == null) return null;
        return body.string();
    }

    private void handleErrorCodes(Response response, String body) {
        if (response.code() == HttpURLConnection.HTTP_UNAUTHORIZED || response.code() == HttpURLConnection.HTTP_FORBIDDEN) {
            throw new AuthProviderException("Access denied", body);
        } else if (response.code() == HttpURLConnection.HTTP_NOT_FOUND) {
            throw new AuthProviderException("Invalid URL: " + this.authProviderUrl);
        } else if (response.code() < 200 || response.code() >= 400) {
            throw new AuthProviderException("Unknown error: " + response, body);
        }
    }

    public static class AuthProviderException extends RuntimeException {
        private final String body;

        public AuthProviderException(Throwable cause) {
            super(cause);
            this.body = null;
        }

        public AuthProviderException(String message) {
            this(message, null);
        }

        public AuthProviderException(String message, String body) {
            super(message);
            this.body = body;
        }

        @Override
        public String getMessage() {
            if (body == null) {
                return super.getMessage();
            }
            return super.getMessage() + "\n" + body;
        }
    }

}
