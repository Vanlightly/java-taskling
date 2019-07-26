package com.siiconcatel.taskling.sqlserver.tokens.executions;

import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.GrantStatus;
import com.siiconcatel.taskling.core.utils.StringUtils;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.DbOperationsService;
import com.siiconcatel.taskling.sqlserver.ancilliaryservices.NamedParameterStatement;
import com.siiconcatel.taskling.sqlserver.tokens.CommonTokenRepository;
import com.siiconcatel.taskling.sqlserver.tokens.QueriesTokens;
import com.siiconcatel.taskling.sqlserver.tokens.TaskExecutionState;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class ExecutionTokenRepositoryMsSql extends DbOperationsService implements ExecutionTokenRepository {
    private final CommonTokenRepository commonTokenRepository;

    public ExecutionTokenRepositoryMsSql(CommonTokenRepository commonTokenRepository)
    {
        this.commonTokenRepository = commonTokenRepository;
    }

    public TokenResponse tryAcquireExecutionToken(TokenRequest tokenRequest)
    {
        TokenResponse response = new TokenResponse();
        response.setStartedAt(Instant.now());

        try (Connection connection = createNewConnection(tokenRequest.getTaskId())) {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            acquireRowLock(tokenRequest.getTaskDefinitionId(), tokenRequest.getTaskExecutionId(), connection);
            ExecutionTokenList tokens = getTokens(tokenRequest.getTaskDefinitionId(), connection);
            boolean adjusted = adjustTokenCount(tokens, tokenRequest.getConcurrencyLimit());
            ExecutionToken assignableToken = getAssignableToken(tokens, connection);
            if (assignableToken == null)
            {
                response.setGrantStatus(GrantStatus.Denied);
                response.setExecutionTokenId("0");
            }
            else
            {
                assignToken(assignableToken, tokenRequest.getTaskExecutionId());
                response.setGrantStatus(GrantStatus.Granted);
                response.setExecutionTokenId(assignableToken.getTokenId());
                adjusted = true;
            }

            if (adjusted)
                persistTokens(tokenRequest.getTaskDefinitionId(), tokens, connection);

            connection.commit();
            return response;
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to acquire execution token", e);
        }
    }

    public void returnExecutionToken(TokenRequest tokenRequest, String executionTokenId)
    {
        try (Connection connection = createNewConnection(tokenRequest.getTaskId())) {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            acquireRowLock(tokenRequest.getTaskDefinitionId(), tokenRequest.getTaskExecutionId(), connection);
            ExecutionTokenList tokens = getTokens(tokenRequest.getTaskDefinitionId(), connection);
            setTokenAsAvailable(tokens, executionTokenId);
            persistTokens(tokenRequest.getTaskDefinitionId(), tokens, connection);

            connection.commit();
        }
        catch(SQLException e) {
            throw new TasklingExecutionException("Failure when trying to return execution token", e);
        }
    }

    private void acquireRowLock(int taskDefinitionId, String taskExecutionId, Connection connection) throws SQLException
    {
        commonTokenRepository.acquireRowLock(taskDefinitionId, taskExecutionId, connection);
    }

    private ExecutionTokenList getTokens(int taskDefinitionId, Connection connection) throws SQLException
    {
        String tokensString = getTokensString(taskDefinitionId, connection);
        return parseTokensString(tokensString);
    }

    public static ExecutionTokenList parseTokensString(String tokensString)
    {
        if (StringUtils.isNullOrEmpty(tokensString))
            return returnDefaultTokenList();

        ExecutionTokenList tokenList = new ExecutionTokenList();

        String[] tokens = tokensString.split("\\|");
        for (String tokenText : tokens)
        {
            ExecutionToken token = new ExecutionToken();
            String[] tokenParts = tokenText.split(",");
            if (tokenParts.length != 3)
                throw new TasklingExecutionException("Token text not valid. Format is I:<id>,G:<granted TaskExecutionId>,S:<status> Invalid text: " + tokensString);

            for (String part : tokenParts)
            {
                if (part.startsWith("I:") && part.length() > 2)
                    token.setTokenId(part.substring(2));
                else if (part.startsWith("G:") && part.length() > 2)
                    token.setGrantedToExecution(part.substring(2));
                else if (part.startsWith("S:") && part.length() > 2)
                    token.setStatus(ExecutionTokenStatus.valueOf(Integer.parseInt(part.substring(2))));
                else
                    throw new TasklingExecutionException("Token text not valid. Format is I:<id>,G:<granted TaskExecutionId>,S:<status> Invalid text: " + tokensString);
            }

            tokenList.getTokens().add(token);
        }

        return tokenList;
    }

    private boolean adjustTokenCount(ExecutionTokenList tokenList, int concurrencyCount)
    {
        boolean modified = false;

        if (concurrencyCount == -1 || concurrencyCount == 0) // if there is no limit
        {
            if (tokenList.getTokens().size() != 1 || (tokenList.getTokens().size() == 1
                    && tokenList.getTokens().stream().allMatch(x -> x.getStatus() != ExecutionTokenStatus.Unlimited)))
            {
                tokenList.getTokens().clear();
                tokenList.getTokens().add(new ExecutionToken(
                    UUID.randomUUID().toString(),
                    ExecutionTokenStatus.Unlimited,
                    "0"));

                modified = true;
            }
        }
        else
        {
            // if has a limit then remove any unlimited tokens
            if (tokenList.getTokens().stream().anyMatch(x -> x.getStatus() == ExecutionTokenStatus.Unlimited))
            {
                tokenList.setTokens(tokenList.getTokens().stream().filter(x -> x.getStatus() != ExecutionTokenStatus.Unlimited).collect(Collectors.toList()));
                modified = true;
            }

            // the current token count is less than the limit then add new tokens
            if (tokenList.getTokens().size() < concurrencyCount)
            {
                while (tokenList.getTokens().size() < concurrencyCount)
                {
                    tokenList.getTokens().add(new ExecutionToken(
                            UUID.randomUUID().toString(),
                            ExecutionTokenStatus.Available,
                            "0"));

                    modified = true;
                }
            }
            // if the current token count is greater than the limit then
            // start removing tokens. Remove Available tokens preferentially.
            else if (tokenList.getTokens().size() > concurrencyCount)
            {
                while (tokenList.getTokens().size() > concurrencyCount)
                {
                    if (tokenList.getTokens().stream().anyMatch(x -> x.getStatus() == ExecutionTokenStatus.Available))
                    {
                        ExecutionToken firstAvailable = tokenList.getTokens().stream()
                                .filter(x -> x.getStatus() == ExecutionTokenStatus.Available)
                                .findFirst().get();
                        tokenList.getTokens().remove(firstAvailable);
                    }
                    else
                    {
                        tokenList.getTokens().remove(tokenList.getTokens().get(0));
                    }

                    modified = true;
                }
            }
        }

        return modified;
    }

    private static ExecutionTokenList returnDefaultTokenList()
    {
        ExecutionTokenList list = new ExecutionTokenList();
        list.getTokens().add(new ExecutionToken(
                UUID.randomUUID().toString(),
                ExecutionTokenStatus.Available));

        return list;
    }

    private String getTokensString(int taskDefinitionId, Connection connection) throws SQLException
    {
        NamedParameterStatement p = new NamedParameterStatement(connection, QueriesTokens.GetExecutionTokensQuery);
        p.setInt("taskDefinitionId", taskDefinitionId);
        ResultSet rs = p.executeQuery();
        if(rs.next()) {
            return rs.getString(1);
        }

        return "";
    }

    private ExecutionToken getAssignableToken(ExecutionTokenList executionTokenList, Connection connection) throws SQLException
    {
        if (hasAvailableToken(executionTokenList))
        {
            return getAvailableToken(executionTokenList);
        }
        else
        {
            List<String> executionIds = executionTokenList.getTokens().stream()
                    .filter(x -> x.getStatus() != ExecutionTokenStatus.Disabled
                            && !StringUtils.isNullOrEmpty(x.getGrantedToExecution()))
                    .map(x -> x.getGrantedToExecution())
                    .collect(Collectors.toList());

            if (executionIds.isEmpty())
                return null;

            List<TaskExecutionState> executionStates = getTaskExecutionStates(executionIds, connection);
            TaskExecutionState expiredExecution = findExpiredExecution(executionStates);
            if (expiredExecution == null)
                return null;

            List<ExecutionToken> tokens = executionTokenList.getTokens().stream()
                    .filter(x -> x.getGrantedToExecution().equals(expiredExecution.getTaskExecutionId()))
                    .collect(Collectors.toList());

            return tokens.get(0);
        }
    }

    private boolean hasAvailableToken(ExecutionTokenList executionTokenList)
    {
        return executionTokenList.getTokens().stream()
                .anyMatch(x -> x.getStatus() == ExecutionTokenStatus.Available
                    || x.getStatus() == ExecutionTokenStatus.Unlimited);
    }

    private ExecutionToken getAvailableToken(ExecutionTokenList executionTokenList)
    {
        return executionTokenList.getTokens().stream()
                .filter(x -> x.getStatus() == ExecutionTokenStatus.Available
                    || x.getStatus() == ExecutionTokenStatus.Unlimited)
                .findFirst().get();
    }

    private List<TaskExecutionState> getTaskExecutionStates(List<String> taskExecutionIds, Connection connection) throws SQLException
    {
        return commonTokenRepository.getTaskExecutionStates(taskExecutionIds, connection);
    }

    private TaskExecutionState findExpiredExecution(List<TaskExecutionState> executionStates)
    {
        for (TaskExecutionState teState : executionStates)
        {
            if (hasExpired(teState))
                return teState;
        }

        return null;
    }

    private boolean hasExpired(TaskExecutionState taskExecutionState)
    {
        return commonTokenRepository.hasExpired(taskExecutionState);
    }

    private void assignToken(ExecutionToken executionToken, String taskExecutionId)
    {
        executionToken.setGrantedToExecution(taskExecutionId);

        if (executionToken.getStatus() != ExecutionTokenStatus.Unlimited)
            executionToken.setStatus(ExecutionTokenStatus.Unavailable);
    }

    private void persistTokens(int taskDefinitionId, ExecutionTokenList executionTokenList, Connection connection) throws SQLException
    {
        String tokenString = generateTokenString(executionTokenList);

        NamedParameterStatement p = new NamedParameterStatement(connection, QueriesTokens.UpdateExecutionTokensQuery);
        p.setInt("taskDefinitionId", taskDefinitionId);
        p.setString("executionTokens", tokenString);
        p.execute();
    }

    private String generateTokenString(ExecutionTokenList executionTokenList)
    {
        StringBuilder sb = new StringBuilder();
        int counter = 0;
        for (ExecutionToken token : executionTokenList.getTokens())
        {
            if (counter > 0)
                sb.append("|");

            sb.append("I:");
            sb.append(token.getTokenId());
            sb.append(",S:");
            sb.append(""+token.getStatus().getNumVal());
            sb.append(",G:");
            sb.append(token.getGrantedToExecution());

            counter++;
        }

        return sb.toString();
    }

    private void setTokenAsAvailable(ExecutionTokenList executionTokenList, String executionTokenId)
    {
        Optional<ExecutionToken> executionToken = executionTokenList.getTokens()
                .stream()
                .filter(x -> x.getTokenId().equals(executionTokenId))
                .findFirst();
        if (executionToken.isPresent() && executionToken.get().getStatus() == ExecutionTokenStatus.Unavailable)
            executionToken.get().setStatus(ExecutionTokenStatus.Available);
    }
}
