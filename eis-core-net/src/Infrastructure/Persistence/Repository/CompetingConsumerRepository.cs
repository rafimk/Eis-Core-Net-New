using System.Data;
using Dapper;
using EisCore.Infrastructure.Persistence.Context;
using EisCore.Infrastructure.Persistence.Contracts;
using Microsoft.Extensions.Logging;

namespace EisCore.Infrastructure.Persistence.Repository;

public class CompetingConsumerRepository : ICompetingConsumerRepository
{
    private readonly DapperContext _context;
    private readonly ILogger<EisEventInboxOutboxRepository> _log;
    private string _hostIp;
    
    
    public CompetingConsumerRepository(DapperContext context, ILogger<EisEventInboxOutboxRepository> log)
    {
        _context = context;
        _log = log;
    }

    public void SetHostIpAddress(string hostIp) => _hostIp = hostIp;

    public async Task<int> InsertEntry(string eisGroupKey)
    {
        try
        {
            var query = "INSERT INTO EIS_COMPETING_CONSUMER_GROUP (ID, GROUP_KEY, HOST_IP_ADDRESS, LAST_ACCESSED_TIMESTAMP) " +
                        " VALUES (@Id, @EisGroupKey, @HostIpAddress, @LastAccessedTimeStamp) ";
        
            var parameters = new DynamicParameters();
        
            var id = Guid.NewGuid().ToString();
        
            var lastAccessedTimeStamp = DateTime.UtcNow;
        
            parameters.Add("Id", id, DbType.String);
            parameters.Add("EisGroupKey", eisGroupKey, DbType.String);
            parameters.Add("HostIpAddress", _hostIp, DbType.String);
            parameters.Add("LastAccessedTimeStamp", lastAccessedTimeStamp, DbType.DateTime);
           
            using var connection = _context.CreateConnection();
            await connection.ExecuteAsync(query, parameters);
        }
        catch (Exception ex)
        {
            _log.LogError("Error occurred: {Ex}", ex.StackTrace);
        }

        return 0;
    }

    public async Task<int> KeepAliveEntry(bool isStarted, string eisGroupKey)
    {
        try
        {
            var query = "UPDATE EIS_COMPETING_CONSUMER_GROUP SET LAST_ACCESSED_TIMESTAMP = CURRENT_TIMESTAMP " + 
                        "WHERE GROUP_KEY = @EisGroupKey AND HOST_IP_ADDRESS = @HostIpAddress AND @StartStatus = 1 ";
        
            var parameters = new DynamicParameters();
            parameters.Add("EisGroupKey", eisGroupKey, DbType.String);
            parameters.Add("HostIpAddress", _hostIp, DbType.String);
            parameters.Add("StartStatus", isStarted, DbType.String);

            using var connection = _context.CreateConnection();
            await connection.ExecuteAsync(query, parameters);
        }
        catch (Exception ex)
        {
            _log.LogError("Error occurred: {Ex}", ex.StackTrace);
        }

        return 0;
    }

    public async Task<int> DeleteStaleEntry(string eisGroupKey, int eisGroupRefreshInterval)
    {
        try
        {
            var query = "DELETE FROM EIS_COMPETING_CONSUMER_GROUP " + 
                        "WHERE EXTRACT(MINUTE FROM (CURRENT_TIMESTAMP - LAST_ACCESSED_TIMESTAMP)) > @EisGroupRefreshInterval " +
                        "AND GROUP_KEY = @EisGroupKey ";
        
            var parameters = new DynamicParameters();
           
            parameters.Add("EisGroupRefreshInterval", eisGroupRefreshInterval, DbType.Int32);
            parameters.Add("EisGroupKey", eisGroupKey, DbType.String);

            using var connection = _context.CreateConnection();
            await connection.ExecuteAsync(query, parameters);
        }
        catch (Exception ex)
        {
            _log.LogError("Error occurred: {Ex}", ex.StackTrace);
        }

        return 0;
    }

    public async Task<string> GetIpAddressOfServer(string eisGroupKey, int eisGroupRefreshInterval)
    {
        try
        {
            var query = "SELECT HOST_IP_ADDRESS FROM EIS_COMPETING_CONSUMER_GROUP WHERE GROUP_KEY= @EisGroupKey " +
                        "AND EXTRACT(MINUTE FROM (CURRENT_TIMESTAMP - LAST_ACCESSED_TIMESTAMP)) <= @EisGroupRefreshInterval ";  
        
            var parameters = new DynamicParameters();
            parameters.Add("EisGroupKey", eisGroupKey, DbType.String);
            parameters.Add("EisGroupRefreshInterval", eisGroupRefreshInterval, DbType.Int32);
        
            using var connection = _context.CreateConnection();
            var hostIpAddress = await connection.QueryAsync<string>(query, parameters);
            return hostIpAddress.First();
        }
        catch (Exception ex)
        {
            _log.LogError("Error occurred: {Ex}", ex.StackTrace);
        }

        return string.Empty;
    }
}