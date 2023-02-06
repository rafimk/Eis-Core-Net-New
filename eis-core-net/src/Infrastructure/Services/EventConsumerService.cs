
using System;
using EisCore.Domain.Entities;
using EisCore.Application.Interfaces;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using Apache.NMS;

using EisCore.Application.Constants;
using EisCore.Infrastructure.Persistence.Contracts;

namespace EisCore
{
    public class EventConsumerService : IEventConsumerService
    {
        private readonly ILogger<EventConsumerService> _log;
        private readonly IConfigurationManager _configManager;
        private IEisEventInboxOutboxRepository _eisEventInboxOutboxRepository;

        public EventConsumerService(ILogger<EventConsumerService> log, IConfigurationManager configurationManager, IEisEventInboxOutboxRepository eisEventInboxOutboxRepository)
        {
            _log = log;
            _configManager = configurationManager;
            _eisEventInboxOutboxRepository = eisEventInboxOutboxRepository;
        }

        public void RunConsumerEventListener(IMessageConsumer consumer)
        {
            _log.LogInformation("called RunConsumerEventListener()");
            //consumer.Listener += new MessageListener(OnMessage);

        }

    
    }
}