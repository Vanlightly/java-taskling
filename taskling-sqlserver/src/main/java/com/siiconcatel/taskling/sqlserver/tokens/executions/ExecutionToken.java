package com.siiconcatel.taskling.sqlserver.tokens.executions;

public class ExecutionToken {
    private String tokenId;
    private ExecutionTokenStatus status;
    private String grantedToExecution;

    public ExecutionToken() {}

    public ExecutionToken(String tokenId, ExecutionTokenStatus status) {
        this.tokenId = tokenId;
        this.status = status;
    }

    public ExecutionToken(String tokenId, ExecutionTokenStatus status, String grantedToExecution) {
        this.tokenId = tokenId;
        this.status = status;
        this.grantedToExecution = grantedToExecution;
    }

    public String getTokenId() {
        return tokenId;
    }

    public void setTokenId(String tokenId) {
        this.tokenId = tokenId;
    }

    public ExecutionTokenStatus getStatus() {
        return status;
    }

    public void setStatus(ExecutionTokenStatus status) {
        this.status = status;
    }

    public String getGrantedToExecution() {
        return grantedToExecution;
    }

    public void setGrantedToExecution(String grantedToExecution) {
        this.grantedToExecution = grantedToExecution;
    }
}
