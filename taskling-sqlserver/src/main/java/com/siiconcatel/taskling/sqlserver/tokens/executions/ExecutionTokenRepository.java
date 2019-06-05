package com.siiconcatel.taskling.sqlserver.tokens.executions;

public interface ExecutionTokenRepository {
    TokenResponse tryAcquireExecutionToken(TokenRequest tokenRequest);
    void returnExecutionToken(TokenRequest tokenRequest, String executionTokenId);
}
