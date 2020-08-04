using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Configuration;
using System.Text;


namespace ChangeFeedFunctions
{
    public class CountResult
    {
        public string id { get; set; }
        public long ItemId { get; set; }
        public long BuyCount { get; set; }
    }

    public static class AnalyticsFunction
    {
        private static string _endpointUrl;
        private static string _primaryKey;

        private static string _databaseId;
        private static string _containerId;
        private static CosmosClient cosmosClient;
        private static string _eventHubConnection;
        private static string _eventHubName;

        [FunctionName("AnalyticsFunction")]
        public static async Task Run([CosmosDBTrigger(
            databaseName: "movies",
            collectionName: "IncomingOrders",
            ConnectionStringSetting = "openhackcosmos1_DOCUMENTDB",
            CreateLeaseCollectionIfNotExists = true,
            LeaseCollectionName = "leases")]IReadOnlyList<Document> input, ILogger log, ExecutionContext context)
        {
            
            var config = new ConfigurationBuilder()
				.SetBasePath(context.FunctionAppDirectory)
				.AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
				.AddEnvironmentVariables()
				.Build();
 
			_primaryKey  = config["openhackcosmos1_DOCUMENTDBPK"];
            _endpointUrl = config["openhackcosmos1_DBURL"];
            _databaseId  = config["openhackcosmos1_DBID"];
            _containerId = config["openhackcosmos1_PRODUCTCOUNTCONT"];
            _eventHubConnection  = config["openhackeventhub_CONNECTION"];
            _eventHubName  = config["openhackeventhub_TOPBUYSHUBNAME"];
            
            cosmosClient = new CosmosClient(_endpointUrl, _primaryKey);

            if (input != null && input.Count > 0)
            {
                log.LogInformation("Documents modified " + input.Count);
                log.LogInformation("First document Id " + input[0].Id);

                var incomingCountMap = new Dictionary<long, int>();

                // Process incoming buy count
                foreach (Document doc in input)
                {
                    var json = doc.ToString();  
                    IReadOnlyList<Document> details = doc.GetPropertyValue<IReadOnlyList<Document>>("Details");
                    foreach (Document detailDoc in details)
                    {
                        long productId = detailDoc.GetPropertyValue<long>("ProductId");
                        int count      = detailDoc.GetPropertyValue<int>("Quantity");

                        log.LogInformation("Detail: " + productId + " - " +count);

                        if (!incomingCountMap.ContainsKey(productId))
                        {
                            incomingCountMap.Add(productId, count);
                        }
                        else
                        {
                            int prev = incomingCountMap.GetValueOrDefault(productId, 0);
                            incomingCountMap.Remove(productId);
                            incomingCountMap.Add(productId, count + prev);
                        }                        
                    }
                
                    log.LogInformation("received doc: " + doc);
                }

                // Get existing buy count
                var db = cosmosClient.GetDatabase(_databaseId);
                var container = db.GetContainer(_containerId);

                QueryDefinition queryDefinition = new QueryDefinition("SELECT c.id, c.ItemId, c.BuyCount FROM c order by c.BuyCount");
                FeedIterator<CountResult> queryResultSetIterator = container.GetItemQueryIterator<CountResult>(queryDefinition);

                Dictionary<long, CountResult> resultsCountMap = new Dictionary<long, CountResult>();
                while (queryResultSetIterator.HasMoreResults)
                {
                    FeedResponse<CountResult> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                    foreach (CountResult curResult in currentResultSet)
                    {
                        resultsCountMap.Add(curResult.ItemId, curResult);
                        //Console.WriteLine(result.Id  + " - " + result.ItemId + " - " + result.BuyCount);
                    }
                }

                var tasks = new List<Task>();
                CountResult result;
                foreach (long itemId in incomingCountMap.Keys)
                {                    
                    if (resultsCountMap.ContainsKey(itemId))
                    {
                        result = resultsCountMap.GetValueOrDefault(itemId);
                        result.BuyCount += incomingCountMap.GetValueOrDefault(result.ItemId);
                    }
                    else
                    {
                        result  = new CountResult();
                        result.ItemId = result.ItemId;
                        result.BuyCount = incomingCountMap.GetValueOrDefault(result.ItemId);
                    }

                    tasks.Add(container.UpsertItemAsync(result, new Microsoft.Azure.Cosmos.PartitionKey(result.ItemId)));
                }

                await Task.WhenAll(tasks);
                log.LogInformation("Writing to cosmos");



                var sbEventHubConnection = new EventHubsConnectionStringBuilder(_eventHubConnection)
                {
                    EntityPath = _eventHubName
                };

                // Read top 10 from cosmos
                var eventHubClient = EventHubClient.CreateFromConnectionString(sbEventHubConnection.ToString());

                tasks = new List<Task>();
                queryDefinition = new QueryDefinition("SELECT top 10 c.BuyCount, c.ItemId, c.id FROM c order by c.BuyCount desc");
                queryResultSetIterator = container.GetItemQueryIterator<CountResult>(queryDefinition);

                while (queryResultSetIterator.HasMoreResults)
                {
                    FeedResponse<CountResult> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                    foreach (CountResult curResult in currentResultSet)
                    {

                        CountResult eventResult = new CountResult();
                        eventResult.BuyCount = curResult.BuyCount;
                        eventResult.id   = curResult.ItemId.ToString();

                        string jsonString = JsonSerializer.Serialize(eventResult);
                        var eventData = new EventData(Encoding.UTF8.GetBytes(jsonString));

                        log.LogInformation("Created Event: " + jsonString);
                        tasks.Add(eventHubClient.SendAsync(eventData));
                    }
                }

                await Task.WhenAll(tasks);
                log.LogInformation("Writing to event hub");            
            }
        }

    }
}