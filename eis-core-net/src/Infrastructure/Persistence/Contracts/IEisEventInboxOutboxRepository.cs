using EisCore.Domain.Entities;

namespace EisCore.Infrastructure.Persistence.Contracts;

public interface IEisEventInboxOutboxRepository
{
    Task<int> TryEventInsert(EisEvent eisEvent, string topicQueueName, string direction);
    Task<int> UpdateEventStatus(string eventId, string eventStatus, string direction);
    Task<IEnumerable<EisEventInboxOutbox>> GetAllUnprocessedEvents(string direction);
}