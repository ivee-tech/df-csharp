using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Azure.Storage.Blobs;

namespace Cats.Function
{
    public static class DownloadCatsOrchestrator
    {
        [FunctionName("DownloadCatsOrchestrator")]
        public static async Task RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {

            var n = 1000;
            var tasks = new List<Task>();

            for (var i = 0; i < n; i++)
            {
                var catId = $"cat{i}";
                tasks.Add(context.CallActivityAsync("ProcessCat", catId));
            }

            await Task.WhenAll(tasks);
        }

        [FunctionName("ProcessCat")]
        public static async Task ProcessCat([ActivityTrigger] string catId, ILogger log)
        {
            var catData = await GetCatData(catId);
            if(catData != null)
            {
                await DownloadAndUploadCatImage(catData, log);
            }
            else
            {
                log.LogInformation($"No cat data found for catId: {catId}");
            }
        }

        private static async Task<dynamic> GetCatData(string catId)
        {
            using (var httpClient = new HttpClient())
            {
                var response = await httpClient.GetAsync($"https://api.thecatapi.com/v1/images/search");

                if (response.IsSuccessStatusCode)
                {
                    var content = await response.Content.ReadAsStringAsync();
                    var catData = JsonConvert.DeserializeObject<List<dynamic>>(content);

                    if (catData.Count > 0)
                    {
                        return catData[0];
                    }
                }

                return null;
            }
        }

        private static async Task DownloadAndUploadCatImage(dynamic catData, ILogger log)
        {
            var connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            var id = catData.id.ToString();
            var url = catData.url.ToString();
            log.LogInformation($"cat data url: {url}");

            if (!string.IsNullOrEmpty(url))
            {
                using (var httpClient = new HttpClient())
                {
                    var response = await httpClient.GetAsync(url);
                    using (var stream = await response.Content.ReadAsStreamAsync())
                    {
                        var blobContainerClient = new BlobContainerClient(connectionString, "cats");
                        var blobClient = blobContainerClient.GetBlobClient($"{id}.jpg");

                        await blobClient.UploadAsync(stream, overwrite: true);
                    }
                }
            }
        }

        [FunctionName("DownloadCatsOrchestrator_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("DownloadCatsOrchestrator", null);

            log.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}