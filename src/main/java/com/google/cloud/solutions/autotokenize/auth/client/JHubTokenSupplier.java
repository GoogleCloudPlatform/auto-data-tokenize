package com.google.cloud.solutions.autotokenize.auth.client;

import java.util.function.Supplier;

public class JHubTokenSupplier implements Supplier<String> {

    private static final String JHUB_API_KEY = "JUPYTERHUB_API_TOKEN";

    @Override
    public String get() {
        return System.getenv(JHUB_API_KEY);
    }
}
