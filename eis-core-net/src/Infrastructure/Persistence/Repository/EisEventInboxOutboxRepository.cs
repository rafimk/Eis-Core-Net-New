using System.Data;
using Dapper;
using EisCore.Domain.Entities;
using EisCore.Infrastructure.Persistence.Context;
using EisCore.Infrastructure.Persistence.Contracts;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace EisCore.Infrastructure.Persistence.Repository;

public class EisEventInboxOutboxRepository : IEisEventInboxOutboxRepository
{
    private readonly DapperContext _context;
    private readonly ILogger<EisEventInboxOutboxRepository> _log;
    
    public EisEventInboxOutboxRepository(DapperContext context, ILogger<EisEventInboxOutboxRepository> log)
    {
        _context = context;
        _log = log;
    }

    public async Task<int> TryEventInsert(EisEvent eisEvent, string topicQueueName, string direction)
    {
        try
        {
            var query = "INSERT INTO EIS_EVENT_INBOX_OUTBOX(ID,EVENT_ID,TOPIC_QUEUE_NAME,EIS_EVENT, EVENT_TIMESTAMP,IN_OUT) " +
                        " VALUES (@Id, @EventID, @TopicQueueName, @EisEvent, @EventTimeStamp, @InOut) ";
        
            var parameters = new DynamicParameters();
        
            var id = Guid.NewGuid().ToString();
        
            var eventContext = JsonConvert.SerializeObject(
                eisEvent,
                new JsonSerializerSettings
                {
                    TypeNameHandling = TypeNameHandling.All
                });
            var eventTimeStamp = DateTime.UtcNow;
        
            parameters.Add("Id", id, DbType.String);
            parameters.Add("EventID", eisEvent.EventID, DbType.String);
            parameters.Add("TopicQueueName", topicQueueName, DbType.String);
            parameters.Add("EisEvent", eventContext, DbType.Binary);
            parameters.Add("EventTimeStamp", eventTimeStamp, DbType.DateTime);
            parameters.Add("InOut", direction, DbType.String);
            
            using var connection = _context.CreateConnection();
            await connection.ExecuteAsync(query, parameters);
        }
        catch (Exception ex)
        {
            _log.LogError("Error occurred: {Ex}", ex.StackTrace);
        }

        return 0;
    }

    public async Task<int> UpdateEventStatus(string eventId, string eventStatus, string direction)
    {
        try
        {
            var query = "UPDATE EIS_EVENT_INBOX_OUTBOX SET IS_EVENT_PROCESSED = @ProcessedStatus " + 
                        "WHERE EVENT_ID = @EventId AND IN_OUT = @Direction ";
        
            var parameters = new DynamicParameters();
            parameters.Add("ProcessedStatus", eventStatus, DbType.String);
            parameters.Add("EventId", eventId, DbType.String);
            parameters.Add("Direction", direction, DbType.String);

            using var connection = _context.CreateConnection();
            await connection.ExecuteAsync(query, parameters);
        }
        catch (Exception ex)
        {
            _log.LogError("Error occurred: {Ex}", ex.StackTrace);
        }

        return 0;
    }

    public async Task<IEnumerable<EisEventInboxOutbox>> GetAllUnprocessedEvents(string direction)
    {
        try
        {
            var query = "SELECT ID, EVENT_ID AS EVENTID, TOPIC_QUEUE_NAME AS TOPICQUEUENAME, EIS_EVENT AS EISEVENT, " + 
                        "EVENT_TIMESTAMP AS EVENTTIMESTAMP, IS_EVENT_PROCESSED AS ISEVENTPROCESSED, IN_OUT AS INOUT " + 
                        "FROM EIS_EVENT_INBOX_OUTBOX WHERE IS_EVENT_PROCESSED IS NULL AND IN_OUT = @Direction " + 
                        "ORDER BY EVENT_TIMESTAMP ASC ";
        
            var parameters = new DynamicParameters();
            parameters.Add("Direction", direction, DbType.String);
        
            using var connection = _context.CreateConnection();
            var eisEventList = await connection.QueryAsync<EisEventInboxOutbox>(query, parameters);
            return eisEventList.ToList();
        }
        catch (Exception ex)
        {
            _log.LogError("Error occurred: {Ex}", ex.StackTrace);
        }

        return new List<EisEventInboxOutbox>();
    }
}