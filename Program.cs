// ======================================================================================
//  This file is part of the Microsoft Dynamics CRM SDK code samples.
//
//  Copyright (C) Microsoft Corporation.  All rights reserved.
//
//  This source code is intended only as a supplement to Microsoft Development Tools and/or 
//  on-line documentation.  See these other materials for detailed information regarding 
//  Microsoft code samples.
//
//  THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER 
//  EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES OF 
//  MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
// ======================================================================================
using Microsoft.Crm.Sdk.Samples.HelperCode;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;

namespace Microsoft.Crm.Sdk.Samples
{
    /// <summary>
    /// This program performs basic create, retrieve, update, and delete (CRUD) and related
    /// operations against a CRM Online 2016 or later organization using the Web API.
    /// </summary>
    /// <remarks>
    /// Before building this application, you must first modify the following configuration 
    /// information in the app.config file:
    ///   - All deployments: Provide connection string service URL's for your organization.
    ///   - CRM Online: Replace the app settings with the correct values for your Azure 
    ///                 application registration. 
    /// See the provided app.config file for more information. 
    /// </remarks>
    internal class BatchOperations
    {
        private const string PREFER_HEADER = "odata.include-annotations=\"OData.Community.Display.V1.FormattedValue\"";
        //Provides a persistent client-to-CRM server communication channel.
        private HttpClient _httpClient;
        //Start with base version and update with actual version later.
        private Version _webAPIVersion = new Version(8, 2);

        //Centralized collection of entity URIs used to manage lifetimes.
        private List<string> _entityUris = new List<string>();

        //A set of variables to hold the state of and URIs for primary entity instances.
        private JObject contact1 = new JObject(), contact2 = new JObject();

        private HttpMessageContent CreateHttpMessageContent(HttpMethod httpMethod, string requestUri, int contentId = 0, string content = null)
        {
            string baseUrl = _httpClient.BaseAddress.AbsoluteUri.Trim() + GetVersionedWebAPIPath();
            if (!requestUri.StartsWith(baseUrl))
            {
                requestUri = baseUrl + requestUri;
            }
            HttpRequestMessage requestMessage = new HttpRequestMessage(httpMethod, requestUri);
            HttpMessageContent messageContent = new HttpMessageContent(requestMessage);
            messageContent.Headers.Remove("Content-Type");
            messageContent.Headers.Add("Content-Type", "application/http");
            messageContent.Headers.Add("Content-Transfer-Encoding", "binary");

            // only GET request requires Accept header
            if (httpMethod == HttpMethod.Get)
            {
                requestMessage.Headers.Add("Accept", "application/json");
            }
            else
            {
                // request other than GET may have content, which is normally JSON
                if (!string.IsNullOrEmpty(content))
                {
                    StringContent stringContent = new StringContent(content);
                    stringContent.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json;type=entry");
                    requestMessage.Content = stringContent;
                }
                messageContent.Headers.Add("Content-ID", contentId.ToString());
            }
            return messageContent;
        }

        private async Task<List<HttpResponseMessage>> SendBatchRequestAsync(List<HttpMessageContent> httpContents)
        {
            if (httpContents == null)
            {
                throw new ArgumentNullException(nameof(httpContents));
            }
            string batchName = $"batch_{Guid.NewGuid()}";
            MultipartContent batchContent = new MultipartContent("mixed", batchName);
            string changesetName = $"changeset_{Guid.NewGuid()}";
            MultipartContent changesetContent = new MultipartContent("mixed", changesetName);
            httpContents.ForEach((c) => changesetContent.Add(c));
            batchContent.Add(changesetContent);
            return await SendBatchRequestAsync(batchContent);
        }

        private async Task<List<HttpResponseMessage>> SendBatchRequestAsync(MultipartContent batchContent)
        {
            HttpRequestMessage batchRequest = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                RequestUri = new Uri(_httpClient.BaseAddress.AbsoluteUri.Trim() + GetVersionedWebAPIPath() + "$batch")
            };
            batchRequest.Content = batchContent;
            batchRequest.Headers.Add("Prefer", PREFER_HEADER);
            batchRequest.Headers.Add("OData-MaxVersion", "4.0");
            batchRequest.Headers.Add("OData-Version", "4.0");
            batchRequest.Headers.Add("Accept", "application/json");
            HttpResponseMessage response = await _httpClient.SendAsync(batchRequest);

            MultipartMemoryStreamProvider body = await response.Content.ReadAsMultipartAsync();
            List<HttpResponseMessage> contents = await ReadHttpContents(body);

            return contents;
        }

        private async Task<List<HttpResponseMessage>> ReadHttpContents(MultipartMemoryStreamProvider body)
        {
            List<HttpResponseMessage> results = new List<HttpResponseMessage>();
            if (body?.Contents != null)
            {
                foreach (HttpContent c in body.Contents)
                {
                    if (c.IsMimeMultipartContent())
                    {
                        results.AddRange(await ReadHttpContents((await c.ReadAsMultipartAsync())));
                    }
                    else if (c.IsHttpResponseMessageContent())
                    {
                        HttpResponseMessage responseMessage = await c.ReadAsHttpResponseMessageAsync();
                        if (responseMessage != null)
                        {
                            results.Add(responseMessage);
                        }
                    }
                    else
                    {
                        HttpResponseMessage responseMessage = DeserializeToResponse(await c.ReadAsStreamAsync());
                        if (responseMessage != null)
                        {
                            results.Add(responseMessage);
                        }
                    }
                }
            }
            return results;
        }

        private HttpResponseMessage DeserializeToResponse(Stream stream)
        {
            HttpResponseMessage response = new HttpResponseMessage();
            MemoryStream memoryStream = new MemoryStream();
            stream.CopyTo(memoryStream);
            response.Content = new ByteArrayContent(memoryStream.ToArray());
            response.Content.Headers.Add("Content-Type", "application/http;msgtype=response");
            return response.Content.ReadAsHttpResponseMessageAsync().Result;
        }

        private string GetVersionedWebAPIPath()
        {
            return string.Format("v{0}/", _webAPIVersion.ToString(2));
        }

        public async Task GetWebAPIVersion()
        {

            HttpRequestMessage RetrieveVersionRequest = new HttpRequestMessage(HttpMethod.Get, GetVersionedWebAPIPath() + "RetrieveVersion");

            HttpResponseMessage RetrieveVersionResponse = await _httpClient.SendAsync(RetrieveVersionRequest);
            if (RetrieveVersionResponse.StatusCode == HttpStatusCode.OK)  //200
            {
                JObject retrievedVersion = JsonConvert.DeserializeObject<JObject>(await RetrieveVersionResponse.Content.ReadAsStringAsync());
                //Capture the actual version available in this organization
                _webAPIVersion = Version.Parse((string)retrievedVersion.GetValue("Version"));
                Console.WriteLine("RetrieveVersion: {0}", retrievedVersion.GetValue("Version"));
            }
            else
            {
                Console.WriteLine("Failed to retrieve the version for reason: {0}", RetrieveVersionResponse.ReasonPhrase);
                throw new CrmHttpResponseException(RetrieveVersionResponse.Content);
            }
        }

        public async Task Sample1Async()
        {
            string batchName = $"batch_{Guid.NewGuid()}";
            MultipartContent batchContent = new MultipartContent("mixed", batchName);

            string changesetName = $"changeset_{Guid.NewGuid()}";
            MultipartContent changesetContent = new MultipartContent("mixed", changesetName);

            contact1.Add("firstname", "Peter");
            contact1.Add("lastname", "Cambel");

            changesetContent.Add(CreateHttpMessageContent(HttpMethod.Post, "contacts", 1, contact1.ToString()));

            contact2.Add("firstname", "Susie");
            contact2.Add("lastname", "Curtis");
            contact2.Add("jobtitle", "Coffee Master");
            contact2.Add("annualincome", 48000);

            changesetContent.Add(CreateHttpMessageContent(HttpMethod.Post, "contacts", 2, contact2.ToString()));

            batchContent.Add(changesetContent);
            batchContent.Add(CreateHttpMessageContent(HttpMethod.Get, "Account_Tasks?$select=subject"));

            List<HttpResponseMessage> responses = await SendBatchRequestAsync(batchContent);
            HandleResponses(responses);
        }

        public async Task Sample2Async()
        {
            List<HttpMessageContent> httpContents = new List<HttpMessageContent>();
            for (int i = 1; i <= 5; i++)
            {
                JObject taskJson = new JObject
                {
                    { "subject", $"Task {i} in batch" },
                    { "description", "Tackle Business Complexity" }
                };

                httpContents.Add(CreateHttpMessageContent(HttpMethod.Post, "tasks", i, taskJson.ToString()));
            }
            List<HttpResponseMessage> responses = await SendBatchRequestAsync(httpContents);
            HandleResponses(responses);
        }

        private async void HandleResponses(List<HttpResponseMessage> responses)
        {
            foreach (HttpResponseMessage response in responses)
            {
                if (response.StatusCode == HttpStatusCode.NoContent)  //204
                {
                    string entityUri = response.Headers.GetValues("OData-EntityId").FirstOrDefault();
                    _entityUris.Add(entityUri);
                    Console.WriteLine("Entity URI: {0}", entityUri);
                }
                else if (response.StatusCode == HttpStatusCode.OK)  //200
                {
                    Console.WriteLine("Request was successful: {0}", await response.Content.ReadAsStringAsync());
                }
                else
                {
                    Console.WriteLine("Request failed for reason: {0}", response.ReasonPhrase);
                }
            }
        }

        /// <summary> 
        ///Provides the high-level logical flow of the program, as well as a section 
        /// containing cleanup code.
        /// </summary>
        public async Task RunAsync()
        {
            try
            {
                await GetWebAPIVersion();
                await Sample1Async();
                await Sample2Async();
            }
            catch (Exception ex)
            {
                DisplayException(ex);
            }
            finally
            {
                if (_entityUris.Any())
                {
                    Console.WriteLine("\n--Section 5 started--");
                    //Delete all the created sample entities.  Note that explicit deletion is not required 
                    // for contact tasks because these are automatically cascade-deleted with owner. 
                    Console.Write("\nDo you want these entity records deleted? (y/n) [y]: ");
                    string answer = Console.ReadLine();
                    answer = answer.Trim();
                    if (!(answer.StartsWith("y") || answer.StartsWith("Y") || answer == string.Empty))
                    {
                        _entityUris.Clear();
                    }

                    HttpResponseMessage deleteResponse1;
                    int successCnt = 0, notFoundCnt = 0, failCnt = 0;
                    HttpContent lastBadResponseContent = null;
                    foreach (string ent in _entityUris)
                    {
                        deleteResponse1 = await _httpClient.DeleteAsync(ent);
                        if (deleteResponse1.IsSuccessStatusCode) //200-299
                        {
                            Console.WriteLine("Entity deleted: \n{0}.", ent);
                            successCnt++;
                        }
                        else if (deleteResponse1.StatusCode == HttpStatusCode.NotFound) //404
                        {
                            //May have been deleted by another user or via cascade operation
                            Console.WriteLine("Entity not found: {0}.", ent);
                            notFoundCnt++;
                        }
                        else
                        {
                            Console.WriteLine("Failed to delete: {0}.", ent);
                            failCnt++;
                            lastBadResponseContent = deleteResponse1.Content;
                        }
                    }
                    Console.WriteLine("Entities deleted: {0}, not found: {1}, delete " +
                        "failures: {2}. \n", successCnt, notFoundCnt, failCnt);
                    _entityUris.Clear();
                    if (failCnt > 0)
                    {
                        //Throw last failure
                        throw new CrmHttpResponseException(lastBadResponseContent);
                    }
                }
            }
        }

        public static void Main(string[] args)
        {
            BatchOperations app = new BatchOperations();
            try
            {
                //Read configuration file and connect to specified CRM server.
                app.ConnectToCRM(args);
                Task.WaitAll(Task.Run(async () => await app.RunAsync()));
            }
            catch (System.Exception ex)
            {
                DisplayException(ex);
            }
            finally
            {
                if (app._httpClient != null)
                {
                    app._httpClient.Dispose();
                }
                Console.WriteLine("Press <Enter> to exit the program.");
                Console.ReadLine();
            }
        }

        /// <summary>
        /// Obtains the connection information from the application's configuration file, then 
        /// uses this info to connect to the specified CRM service.
        /// </summary>
        /// <param name="args"> Command line arguments. The first specifies the name of the 
        ///  connection string setting. </param>
        private void ConnectToCRM(string[] cmdargs)
        {
            //Create a helper object to read app.config for service URL and application 
            // registration settings.
            Configuration config = null;
            if (cmdargs.Length > 0)
            {
                config = new FileConfiguration(cmdargs[0]);
            }
            else
            {
                config = new FileConfiguration(null);
            }
            //Create a helper object to authenticate the user with this connection info.
            Authentication auth = new Authentication(config);
            //Next use a HttpClient object to connect to specified CRM Web service.
            _httpClient = new HttpClient(auth.ClientHandler, true)
            {
                //Define the Web API base address, the max period of execute time, the 
                // default OData version, and the default response payload format.
                BaseAddress = new Uri(config.ServiceUrl + "api/data/"),
                Timeout = new TimeSpan(0, 2, 0)
            };
            _httpClient.DefaultRequestHeaders.Add("OData-MaxVersion", "4.0");
            _httpClient.DefaultRequestHeaders.Add("OData-Version", "4.0");
            _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        }

        /// <summary> Helper method to display caught exceptions </summary>
        private static void DisplayException(Exception ex)
        {
            Console.WriteLine("The application terminated with an error.");
            Console.WriteLine(ex.Message);
            while (ex.InnerException != null)
            {
                Console.WriteLine("\t* {0}", ex.InnerException.Message);
                ex = ex.InnerException;
            }
        }
    }
}
