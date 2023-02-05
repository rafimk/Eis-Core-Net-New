using System.Text.Json;
using EisCore.Application.Constants;
using EisCore.Application.Interfaces;
using EisCore.Application.Util;
using EisCore.Domain.Entities;
using EisCore.Infrastructure.Persistence.Contracts;
using Microsoft.Extensions.Logging;

namespace EisCore
{
    public class MessageQueueManager : IMessageQueueManager
    {
        private readonly IBrokerConnectionFactory _brokerConnectionFactory;
        private readonly ICompetingConsumerRepository _competingConsumerRepository;
        private readonly IEisEventInboxOutboxRepository _eisEventInboxOutboxRepository;
        private readonly IConfigurationManager _configManager;
        private readonly ILogger<MessageQueueManager> _log;
        private readonly EventHandlerRegistry _eventRegistry;

        private string sourceName;

        //TODO testing purposes
        private string testHostIp;

        public MessageQueueManager(IBrokerConnectionFactory brokerConnectionFactory,
            IConfigurationManager configManager,
            ILogger<MessageQueueManager> log,
            EventHandlerRegistry eventRegistry, ICompetingConsumerRepository competingConsumerRepository,
            IEisEventInboxOutboxRepository eisEventInboxOutboxRepository)
        {
            _brokerConnectionFactory = brokerConnectionFactory;
            _configManager = configManager;
            _log = log;
            _eventRegistry = eventRegistry;
            _competingConsumerRepository = competingConsumerRepository;
            _eisEventInboxOutboxRepository = eisEventInboxOutboxRepository;
            sourceName = _configManager.GetSourceSystemName();
            testHostIp = Guid.NewGuid().ToString();
            _competingConsumerRepository.SetHostIpAddress(testHostIp);
            //Check if any Message Processors are registered, then call the keep alive services
            ConsumerKeepAliveTask();
        }

        public async Task InboxOutboxPollerTask()
        {
            if (GlobalVariables.IsCurrentIpLockedForConsumer)
            {
                await ProcessAllUnprocessedInboxEvents(); //Process existing events from db
                if (!GlobalVariables.IsTransportInterrupted)
                {
                    await ProcessAllUnprocessedOutboxEvents(); //Publish the evfents from db to the MQ
                }
                else
                {
                    _log.LogInformation("QuartzInboxOutboxPollerJob >> not locked for broker connection");
                }
            }

            return;
        }

        private async Task ProcessAllUnprocessedInboxEvents()
        {
            var inboxEventsList =
                await _eisEventInboxOutboxRepository.GetAllUnprocessedEvents(AtLeastOnceDeliveryDirection.IN);

            if (inboxEventsList.Any())
            {
                var recordUpdateStatus = 0;
                string _eventID = null;
                _log.LogInformation("INBOX: UnprocessedInboxEvents data are available: {c}", inboxEventsList.Count());
                foreach (var events in inboxEventsList)
                {
                    try
                    {
                        //TODO check null
                        EisEventInboxOutbox dbEvents = events;
                        EisEvent eisEvent = JsonSerializer.Deserialize<EisEvent>(events.EisEvent);
                        _eventID = eisEvent?.EventID;
                        ConsumeEvent(eisEvent, dbEvents.TopicQueueName);
                        recordUpdateStatus = await _eisEventInboxOutboxRepository.UpdateEventStatus(_eventID,
                            TestSystemVariables.PROCESSED, AtLeastOnceDeliveryDirection.IN);
                        _log.LogInformation("Processed {e}, with status {s}", _eventID.ToString(), recordUpdateStatus);
                    }
                    catch (Exception e)
                    {
                        _log.LogError("Exception occurred while processing > {e}", e.StackTrace);
                        recordUpdateStatus = await _eisEventInboxOutboxRepository.UpdateEventStatus(_eventID,
                            TestSystemVariables.FAILED, AtLeastOnceDeliveryDirection.IN);
                    }
                }
            }
            else
            {
                GlobalVariables.IsUnprocessedInMessagePresent = false;
            }
        }


        private async Task ProcessAllUnprocessedOutboxEvents()
        {
            //TODO check thread safety and use BlockingCollection<EisEventInboxOutbox> Collection
            var  outboxEventsList = await _eisEventInboxOutboxRepository.GetAllUnprocessedEvents(AtLeastOnceDeliveryDirection.OUT);
            if (outboxEventsList.Any())
            {
                var recordUpdateStatus = 0;
                string _eventID = null;
                _log.LogInformation("OUTBOX: UnprocessedOutboxEvents data are available: {c}", outboxEventsList.Count());

                foreach (var events in outboxEventsList)
                {
                    try
                    {
                        //TODO check null
                        EisEventInboxOutbox dbEvents = events;
                        EisEvent eisEvent = JsonSerializer.Deserialize<EisEvent>(events.EisEvent);
                        _eventID = eisEvent.EventID;
                        QueueToPublisherTopic(eisEvent, false);
                        recordUpdateStatus = await _eisEventInboxOutboxRepository.UpdateEventStatus(_eventID, TestSystemVariables.PROCESSED,
                            AtLeastOnceDeliveryDirection.OUT);
                        _log.LogInformation("Processed {e}, with status {s}", _eventID.ToString(), recordUpdateStatus);
                    }
                    catch (Exception e)
                    {
                        _log.LogError("Exception occurred while processing > {e}", e.StackTrace);
                        recordUpdateStatus = await _eisEventInboxOutboxRepository.UpdateEventStatus(_eventID, TestSystemVariables.FAILED,
                            AtLeastOnceDeliveryDirection.OUT);
                    }
                }
            }
            else
            {
                GlobalVariables.IsUnprocessedOutMessagePresent = false;
            }
        }


        public async void QueueToPublisherTopic(EisEvent eisEvent, bool isCurrent)
        {
            if (isCurrent && !GlobalVariables.IsUnprocessedOutMessagePresent)
            {
                //****
                var OutboundTopic = _configManager.GetAppSettings().OutboundTopic;

                int recordInsertCount = await _eisEventInboxOutboxRepository.TryEventInsert(eisEvent, OutboundTopic, AtLeastOnceDeliveryDirection.OUT);

                if (recordInsertCount == 1)
                {
                    _log.LogInformation("OUTBOX::NEW [Insert] status: {a}", recordInsertCount);
                    //Console.WriteLine($"publish Thread={Thread.CurrentThread.ManagedThreadId} SendToQueue called");
                    _brokerConnectionFactory.QueueToPublisherTopic(eisEvent);
                    var recordUpdateStatus1 = await _eisEventInboxOutboxRepository.UpdateEventStatus(eisEvent.EventID, TestSystemVariables.PROCESSED, AtLeastOnceDeliveryDirection.OUT);
                    _log.LogInformation("OUTBOX::Processed {e}, with status {s}", eisEvent.EventID.ToString(), recordUpdateStatus1);
                }
                else
                {
                    _log.LogInformation("OUTBOX::OLD record already published. insert status: {a}", recordInsertCount);
                }
            }

            if (!isCurrent)//First publish the messages in OUTBOX queue if not empty
            {
                _brokerConnectionFactory.QueueToPublisherTopic(eisEvent);
                var recordUpdateStatus = await _eisEventInboxOutboxRepository.UpdateEventStatus(eisEvent.EventID, TestSystemVariables.PROCESSED, AtLeastOnceDeliveryDirection.OUT);
                _log.LogInformation("OUTBOX::TIMER::Processed {e}, with status {s}", eisEvent.EventID.ToString(), recordUpdateStatus);
            }
            //If it is coming from timer, isCurrent=false, and GlobalVariables.IsUnprocessedOutMessagePresent is true -- do nothing - let the 
        }
        public void ConsumeEvent(EisEvent eisEvent, string queueName)
        {
            UtilityClass.ConsumeEvent(eisEvent, queueName, _eventRegistry, sourceName, _log);
        }
        public async Task ConsumerKeepAliveTask()
        {
            await Console.Out.WriteLineAsync("#########Consumer Connection Quartz Job... Cron: [" + _configManager.GetBrokerConfiguration().CronExpression + "]");
            var eisGroupKey = _configManager.GetSourceSystemName() + "_COMPETING_CONSUMER_GROUP";
            var refreshInterval = _configManager.GetBrokerConfiguration().RefreshInterval;

            try
            {
                //TODO put the hostIP
                var hostIP = testHostIp;// _dbContext.GetIPAddressOfServer(eisGroupKey, refreshInterval);
                var deleteResult = await _competingConsumerRepository.DeleteStaleEntry(eisGroupKey, refreshInterval);
                _log.LogInformation("Stale entry delete status:{r}", deleteResult);
                var insertResult = await _competingConsumerRepository.InsertEntry(eisGroupKey);

                if (insertResult == 1)
                {
                    _brokerConnectionFactory.CreateConsumerListener();
                    _log.LogInformation("*** Consumer locked for: {Ip} in group: {GroupKey}", hostIP, eisGroupKey);
                    GlobalVariables.IsCurrentIpLockedForConsumer = true;
                }
                else
                {
                    string ipAddress = await _competingConsumerRepository.GetIpAddressOfServer(eisGroupKey, refreshInterval);
                    
                    if (ipAddress != null)
                    {
                        _log.LogInformation($"Current IP: [{testHostIp}]");
                        _log.LogInformation("IsIPAddressMatchesWithGroupEntry(IpAddress): " + IsIpAddressMatchesWithGroupEntry(ipAddress));
                        
                        if (!IsIpAddressMatchesWithGroupEntry(ipAddress))
                        {
                            _brokerConnectionFactory.DestroyConsumerConnection();
                            GlobalVariables.IsCurrentIpLockedForConsumer = false;
                        }
                        else
                        {                            
                            _brokerConnectionFactory.CreateConsumerListener();
                            if (!GlobalVariables.IsTransportInterrupted)
                            {
                                var keepAliveResult = _competingConsumerRepository.KeepAliveEntry(true, eisGroupKey);
                                _log.LogInformation("*** Refreshing Keep Alive entry {K}", keepAliveResult.Result);
                                GlobalVariables.IsCurrentIpLockedForConsumer = true;
                            }
                            else
                            {
                                _log.LogCritical("Broker is down. Connection Interrupted");
                            }
                        }
                    }
                    else
                    {
                        _brokerConnectionFactory.DestroyConsumerConnection();
                        _log.LogInformation("***Connection destroyed");
                    }
                }
                await Console.Out.WriteLineAsync("exiting QuartzKeepAliveEntryJob");
                return;
            }
            catch (Exception ex)
            {
                _log.LogCritical("exception when creating consumer: {Ex}", ex.StackTrace);
                _brokerConnectionFactory.DestroyConsumerConnection();
                _log.LogCritical("Consumer connection stopped on IP: ");
            }
            await Console.Out.WriteLineAsync("exception when creating consumer");
            return;
        }
        private bool IsIpAddressMatchesWithGroupEntry(string ipAddress)
        {
            return ipAddress.Equals(testHostIp);
        }
    }
}