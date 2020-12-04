using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos;

namespace Teapot.Function
{
    public static class CosmosTrigger
    {
        private static readonly string _endpointUrl = System.Environment.GetEnvironmentVariable("endpointUrl");
        private static readonly string _primaryKey = System.Environment.GetEnvironmentVariable("primaryKey");
        private static readonly string _databaseId = "database";
        private static readonly string _containerId = "coll2";
        private static CosmosClient cosmosClient = new CosmosClient(_endpointUrl, _primaryKey);


        [FunctionName("CosmosTrigger")]
        public static async Task Run([CosmosDBTrigger(
            databaseName: "database",
            collectionName: "coll1",
            ConnectionStringSetting = "functioncosmos_DOCUMENTDB",
            LeaseCollectionName = "leases",
            CreateLeaseCollectionIfNotExists = true)]IReadOnlyList<Document> input, ILogger log)
        {
            if (input != null && input.Count > 0)
            {
                var container2 = cosmosClient.GetContainer(_databaseId, _containerId);
                foreach (Document doc in input)
                {
                    log.LogInformation("pusing object to coll2");
                    var a = doc.GetPropertyValue<string>("createdDate");
                    if (a == null) {
                        log.LogInformation("createdDate not set for doc: " + doc.Id);
                        doc.SetPropertyValue("hasCreatedDate", false);
                    } 
                    else 
                    {
                        log.LogInformation("createdDate set for doc: " + doc.Id);
                        doc.SetPropertyValue("hasCreatedDate", true);
                    }
                    await Task.CompletedTask;
                    try
                    {
                        await container2.CreateItemAsync<Document>(doc);
                    }
                    catch (Exception e)
                    {
                        log.LogError("Failed to push into coll2: " + e);
                    }
                }
            }
        }
    }
}
