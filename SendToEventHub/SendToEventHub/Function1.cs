using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventHubs;
using System.Text;
using System;

namespace SendToEventHub
{
    public static class Function1
    {
        private static EventHubClient eventHubClient;
        private const string EhConnectionString = "Endpoint=sb://ecommeventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=2BrfwIoZTVGKZantnQXL5D3NTANuhGcsyDSXUfMcApE=";
        private const string EhEntityPath = "ecommcosmosrumetric";

        [FunctionName("SendToEcommEventHub")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequestMessage req, TraceWriter log)
        {
            log.Info("C# HTTP trigger function processed a request.");

            // parse query parameter
            string name = req.GetQueryNameValuePairs()
                .FirstOrDefault(q => string.Compare(q.Key, "name", true) == 0)
                .Value;

            // Get request body
            dynamic data = await req.Content.ReadAsAsync<object>();

            // Set name to query string or body data
            name = name ?? data?.name;

            InitialiseClient();
            log.Info("Event Hub client initialized.");

            await SendMessagesToEventHub(5, log);

            await eventHubClient.CloseAsync();
            log.Info("Event Hub client closed.");

            

            return name == null
                ? req.CreateResponse(HttpStatusCode.BadRequest, "Please pass a name on the query string or in the request body")
                : req.CreateResponse(HttpStatusCode.OK, "Hello " + name);
        }

        // Creates an event hub client and sends 100 messages to the event hub.
        private static async Task SendMessagesToEventHub(int numMessagesToSend, TraceWriter log)
        {
            for (var i = 0; i < numMessagesToSend; i++)
            {
                try
                {
                    var message = $"Message {i}";
                    log.Info($"Sending message: {message}");
                    await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
                }
                catch (Exception exception)
                {
                    log.Error($"{DateTime.Now} > Exception: {exception.Message}");
                }

                await Task.Delay(10);
            }

            log.Info($"{numMessagesToSend} messages sent.");
        }

        private static void InitialiseClient()
        {
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EhConnectionString)
            {
                EntityPath = EhEntityPath
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            
        }
    }
}
