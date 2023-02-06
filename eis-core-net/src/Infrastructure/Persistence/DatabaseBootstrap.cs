using EisCore.Application.Interfaces;
using Dapper;
using Microsoft.Extensions.Configuration;
using System.Reflection;
using EisCore.Application.Constants;
using EisCore.Domain.Entities;
using EisCore.Infrastructure.Persistence.Contracts;
using Microsoft.Data.SqlClient;

namespace EisCore.Infrastructure.Persistence
{
    public class DatabaseBootstrap : IDatabaseBootstrap
    {

        private string _databaseName;
        private readonly IConfiguration _configuration;
        private readonly IEisEventInboxOutboxRepository _eisEventInboxOutboxRepository;

        public DatabaseBootstrap(IConfiguration configuration, IEisEventInboxOutboxRepository eisEventInboxOutboxRepository)
        {
            _configuration = configuration;
            _eisEventInboxOutboxRepository = eisEventInboxOutboxRepository;
            _databaseName = this._configuration.GetConnectionString("DefaultConnection");
            Setup();
            InitiateUnprocessedINOUTMessages();
        }
        public void Setup()
        {
            Console.WriteLine("Setup...");
            // using var connection = new SqlConnection(_databaseName);
            // try
            // {
            //
            //
            //     //  var table = connection.Query<string>("SELECT name FROM sqlite_master WHERE type='table' AND name = 'TEST_COMPETING_CONSUMER_GROUP';");
            //     //var tableName = table.FirstOrDefault();
            //     // if (!string.IsNullOrEmpty(tableName) && tableName == "TEST_COMPETING_CONSUMER_GROUP")
            //     //     return;
            //
            //
            //     Console.WriteLine("before executing schema.sql ");
            //
            //     var assembly = Assembly.GetExecutingAssembly();
            //     var resourceName = "EisCore.schema.sql";
            //
            //     string result = "";
            //
            //     using (Stream stream = assembly.GetManifestResourceStream(resourceName))
            //     using (StreamReader reader = new StreamReader(stream))
            //     {
            //         result = reader.ReadToEnd();
            //         //Console.WriteLine(result);	
            //     }
            //
            //     //string Sqlscript = File.ReadAllText("schema.sql");
            //     connection.Execute(result);
            //
            //     Console.WriteLine($"Created Dabatase {_databaseName}");
            //
            // }
            // catch (System.Exception e)
            // {
            //     Console.WriteLine("Exception connecting to database: " + e.StackTrace);
            //
            // }
        }

        public async void InitiateUnprocessedINOUTMessages()
        {
            var inboxEventList = await _eisEventInboxOutboxRepository.GetAllUnprocessedEvents(AtLeastOnceDeliveryDirection.IN);
            var outboxEventList = await _eisEventInboxOutboxRepository.GetAllUnprocessedEvents(AtLeastOnceDeliveryDirection.OUT);
            
            GlobalVariables.IsUnprocessedInMessagePresent = inboxEventList.Any();
            GlobalVariables.IsUnprocessedOutMessagePresent = outboxEventList.Any();

        }


    }
}