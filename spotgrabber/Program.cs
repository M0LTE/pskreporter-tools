using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Newtonsoft.Json;

namespace spotgrabber
{
    class Program
    {
        static void Main(string[] args)
        {
            Sender sender = new Sender();
            var senderTask = sender.Start();

            Grabber grabber = new Grabber();
            grabber.SpotsReceived += (s, e) => sender.QueueForSending(e.Spot);
            var grabberTask = grabber.Start();

            Task.WaitAll(grabberTask, senderTask);
        }
    }

    class Sender
    {
        private readonly ConcurrentQueue<Spot> internalQueue = new ConcurrentQueue<Spot>();

        public Sender()
        {
            snsClient = new AmazonSimpleNotificationServiceClient(RegionEndpoint.EUWest2);
        }

        AmazonSimpleNotificationServiceClient snsClient;

        public Task Start()
        {
            return Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    if (internalQueue.TryDequeue(out Spot spot))
                    {
                        var request = new PublishRequest
                        {
                            TopicArn = "arn:aws:sns:eu-west-2:335384886244:pskreporterspots",
                            Message = JsonConvert.SerializeObject(new
                            {
                                spot.SequenceNumber,
                                spot.Frequency,
                                spot.Mode,
                                spot.SpottedUtc,
                                spot.ReceiverCallsign,
                                spot.ReceiverDecoderSoftware,
                                spot.ReceiverLocator,
                                spot.SenderCallsign,
                                spot.SenderLocator,
                            })
                        };

                        Task.Factory.StartNew(async () =>
                        {
                            PublishResponse publishResponse;
                            try
                            {
                                publishResponse = await snsClient.PublishAsync(request);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex.Message);
                                return;
                            }

                            if (publishResponse.HttpStatusCode != HttpStatusCode.OK)
                            {
                                Console.WriteLine(publishResponse.HttpStatusCode);
                            }
                            else
                            {
                                Console.WriteLine(DateTime.Now);
                            }
                        });
                    }
                    else
                    {
                        Thread.Sleep(1000); 
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }

        public void QueueForSending(Spot spot)
        {
            internalQueue.Enqueue(spot);
        }
    }

    class Grabber
    {
        const string url = "https://www.pskreporter.info/stream/report?token=HFYcsJkpeDMt1W2dreuZlJEYwQa9N0y1";

        public event EventHandler<SpotReceivedEventArgs> SpotsReceived;
        private readonly HttpClient httpClient;
       
        public Grabber()
        {
            httpClient = new HttpClient();
            httpClient.Timeout = Timeout.InfiniteTimeSpan;

        }

        internal async Task Start()
        {
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();


            while (true)
            {
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                using (var response = await httpClient.SendAsync(
                    request,
                    HttpCompletionOption.ResponseHeadersRead))
                {
                    using (Stream stream = await response.Content.ReadAsStreamAsync())
                    using (var streamReader = new StreamReader(stream))
                    {
                        while (true)
                        {
                            cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(30));

                            string json;
                            try
                            {
                                json = await streamReader.ReadLineAsync().WithCancellation(cancellationTokenSource.Token);
                            }
                            catch (TaskCanceledException)
                            {
                                break;
                            }

                            Spot spot = Spot.FromJson(json);

                            spot.ReceivedFromPskReporterUtc = DateTime.UtcNow;

                            await Task.Factory.StartNew(() => SpotsReceived(this, new SpotReceivedEventArgs { Spot = spot }));

                            //await Task.Factory.FromAsync((asyncCallback, obj) => SpotsReceived.BeginInvoke(this, new SpotReceivedEventArgs { Spot = spot }, asyncCallback, obj), SpotsReceived.EndInvoke, null);
                        }
                    }

                    Console.WriteLine("No messages for a while. Sleeping...");
                    Thread.Sleep(10000);
                    Console.WriteLine("Reconnecting...");
                }
            }
        }
    }

    public class SpotReceivedEventArgs : EventArgs
    {
        public Spot Spot { get; set; }
    }

    public class Spot
    {
        /*
         * {"sequenceNumber":7498374613,"frequency":14075456,"mode":"FT8","sNR":-7,"flowStartSeconds":1560187709,"senderCallsign":"JA7GBS","senderLocator":"QM08mi","receiverCallsign":"SP5ENA","receiverLocator":"KO02nf09","receiverDecoderSoftware":"JTDX v2.0.1-rc137_6"}      
         */
        internal static Spot FromJson(string v) => JsonConvert.DeserializeObject<Spot>(v);

        public DateTime ReceivedFromPskReporterUtc { get; set; }
        public long SequenceNumber { get; set; }
        public long? Frequency { get; set; }
        public string Mode { get; set; }
        public int? Snr { get; set; }
        public long FlowStartSeconds { get; set; }
        public DateTime SpottedUtc => new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(FlowStartSeconds);
        public string SenderCallsign { get; set; }
        public string SenderLocator { get; set; }
        public string ReceiverCallsign { get; set; }
        public string ReceiverLocator { get; set; }
        public string ReceiverDecoderSoftware { get; set; }

        public override string ToString()
        {
            return $"{SpottedUtc:yyyy-MM-dd HH:mm:ss} age:{(ReceivedFromPskReporterUtc - SpottedUtc).TotalSeconds:0.0}s {SenderCallsign} -> {ReceiverCallsign}";
        }
    }

    public static partial class ExtensionMethods
    {
        public static Task<T> WithCancellation<T>(this Task<T> task, CancellationToken cancellationToken)
        {
            return task.IsCompleted // fast-path optimization
                ? task
                : task.ContinueWith(
                    completedTask => completedTask.GetAwaiter().GetResult(),
                    cancellationToken,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
        }
    }
}
