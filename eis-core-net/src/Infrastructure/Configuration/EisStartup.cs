using Microsoft.Extensions.DependencyInjection;
using EisCore.Application.Interfaces;
using EisCore.Domain.Entities;
using EisCore.Infrastructure.Persistence;
using Quartz;
using EisCore.Infrastructure.Services;
using Quartz.Impl;
using Quartz.Spi;
using System.Collections.Specialized;
using EisCore.Infrastructure.Persistence.Context;
using EisCore.Infrastructure.Persistence.Contracts;
using EisCore.Infrastructure.Persistence.Repository;

namespace EisCore.Infrastructure.Configuration
{
    public static class EisStartup
    {

        public static IServiceCollection AddEisServices(this IServiceCollection services)
        {
            services.AddSingleton<IConfigurationManager, ConfigurationManager>();
            services.AddSingleton<IDatabaseBootstrap, DatabaseBootstrap>();
            
            services.AddSingleton<DapperContext>(); 
            services.AddSingleton<IEisEventInboxOutboxRepository, EisEventInboxOutboxRepository>();
            services.AddSingleton<ICompetingConsumerRepository, CompetingConsumerRepository>();
            
            services.AddSingleton<BrokerConfiguration>();
            services.AddSingleton<EventHandlerRegistry>();
            services.AddSingleton<IBrokerConnectionFactory, BrokerConnectionFactory>();
            services.AddSingleton<IMessageQueueManager, MessageQueueManager>();
            //services.AddSingleton<IEventConsumerService, EventConsumerService>();
            services.AddSingleton<IEventPublisherService, EventPublisherService>();
            services.AddSingleton<IJobFactory, JobFactory>();

            var properties = new NameValueCollection();
            properties["quartz.scheduler.instanceName"] = "ConsumerQuartzScheduler";
            properties["quartz.scheduler.instanceId"] = "ConsumerQuartzInstance";
            properties["quartz.threadPool.threadCount"] = "1";
            services.AddSingleton<ISchedulerFactory>(sf => new StdSchedulerFactory(properties));

            //services.AddSingleton<ISchedulerFactory, StdSchedulerFactory>();
            services.AddHostedService<QuartzHostedService>();
            // base configuration from appsettings.json
            //services.Configure<QuartzOptions>(Configuration.GetSection("Quartz"));
            // if you are using persistent job store, you might want to alter some options
            services.Configure<QuartzOptions>(options =>
            {
                options.SchedulerName = "Quartz ASP.NET Core Test Scheduler";
                options.Scheduling.IgnoreDuplicates = true; // default: false
                options.Scheduling.OverWriteExistingData = true; // default: true
            });

            // Add the required Quartz.NET services
            services.AddSingleton<ConsumerKeepAliveEntryPollerJob>();
            services.AddSingleton<InboxOutboxPollerJob>();
            services.AddSingleton<IJobSchedule, ConsumerKeepAliveJobSchedule>();
            services.AddSingleton<IJobSchedule, InboxOutboxPollerJobSchedule>();
            return services;
            //new JobSchedule( jobType: typeof(QuartzKeepAliveEntryJob),cronExpression:"0/20 * * * * ?")
            //);// run every {n} seconds

        }
    }
}
