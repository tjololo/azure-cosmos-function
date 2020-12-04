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
        private static readonly string _containerId = "coll1";
        private static CosmosClient cosmosClient = new CosmosClient(_endpointUrl, _primaryKey);

        public static (bool needsUpdate, bool value) NeedUpdate(Document doc)
        {
            if (doc.GetPropertyValue<string>("hasCreatedDate") == null)
            {
                return (true, false);
            }
            bool hasCreatedDateValue = doc.GetPropertyValue<bool>("hasCreatedDate");
            bool createdDateDefined = doc.GetPropertyValue<string>("createdDate") != null;
            if (hasCreatedDateValue != createdDateDefined)
            {
                return (true, createdDateDefined);
            }
            return (false, false);
        }

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
                    (bool needsUpdate, bool value) = NeedUpdate(doc);
                    if (needsUpdate)
                    {
                        log.LogInformation("pusing updated object to " + _containerId);
                        doc.SetPropertyValue("hasCreatedDate", value);
                        try
                        {
                            await container2.ReplaceItemAsync<Document>(doc, doc.Id);
                        }
                        catch (Exception e)
                        {
                            log.LogError("Failed to update object in collection: " + e);
                        }
                    }
                }
            }
        }
    }
}
