package com.siiconcatel.taskling.sqlserver.tokens.executions;

import java.util.ArrayList;
import java.util.List;

public class ExecutionTokenList {

    private List<ExecutionToken> tokens;

    public ExecutionTokenList()
    {
        tokens = new ArrayList<>();
    }

    public List<ExecutionToken> getTokens() {
        return tokens;
    }

    public void setTokens(List<ExecutionToken> tokens) {
        this.tokens = tokens;
    }
}
