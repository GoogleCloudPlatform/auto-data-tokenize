package com.google.cloud.solutions.autotokenize.auth.client;

import com.google.gson.annotations.SerializedName;

import java.util.Map;

public class AuthResponse {

    @SerializedName("access_token")
    private String accessToken;
    @SerializedName("exchanged_tokens")
    private Map<String, ExchangedToken> exchangedTokens;

    public AuthResponse(String accessToken, Map<String, ExchangedToken> exchangedTokens) {
        this.accessToken = accessToken;
        this.exchangedTokens = exchangedTokens;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public Map<String, ExchangedToken> getExchangedTokens() {
        return exchangedTokens;
    }

    public class ExchangedToken {
        @SerializedName("access_token")
        private String accessToken;
        private int exp;

        public ExchangedToken(String accessToken, int exp) {
            this.accessToken = accessToken;
            this.exp = exp;
        }

        public String getAccessToken() {
            return accessToken;
        }

        public int getExp() {
            return exp;
        }

        public Long getExpirationTimeMilliSeconds() {
            return System.currentTimeMillis() + (exp * 1000);
        }
    }
}
