namespace EisCore.Infrastructure.Persistence.Contracts;

public interface ICompetingConsumerRepository
{
    void SetHostIpAddress(string hostIp);
    Task<int> InsertEntry(string eisGroupKey);
    Task<int> KeepAliveEntry(bool isStarted, string eisGroupKey);
    Task<int> DeleteStaleEntry(string eisGroupKey, int eisGroupRefreshInterval);
    Task<string> GetIpAddressOfServer(string eisGroupKey, int eisGroupRefreshInterval);
}