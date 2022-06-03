package com.google.cloud.solutions.autotokenize.auth.client;

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Interceptor that adds a <p>Bearer</p> token to a request.
 */
public class BearerTokenInterceptor implements Interceptor {

    private static final String AUTHORIZATION_HEADER_NAME = "Authorization";
    private final Supplier<String> tokenSupplier;

    /**
     * Create new instance
     *
     * @param tokenSupplier the token supplier
     */
    public BearerTokenInterceptor(Supplier<String> tokenSupplier) {
        this.tokenSupplier = Objects.requireNonNull(tokenSupplier);
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request.Builder newRequest = chain.request().newBuilder();
        newRequest.header(AUTHORIZATION_HEADER_NAME, String.format("Bearer %s", tokenSupplier.get()));
        return chain.proceed(newRequest.build());
    }
}