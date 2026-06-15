using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dash.Tests;

/// <summary>
/// A hand-rolled <see cref="HttpMessageHandler"/> that returns
/// canned responses and records each call. We avoid Moq for the
/// handler itself because the call recording we need (URL, headers,
/// body) is easier to express as a queue of explicit responses
/// than as a chain of Moq setups.
/// </summary>
internal sealed class FakeHttpMessageHandler : HttpMessageHandler
{
    private readonly Queue<Func<HttpRequestMessage, HttpResponseMessage>> _responses = new();

    public List<HttpRequestMessage> Requests { get; } = new();
    public List<string?> RequestBodies { get; } = new();

    public FakeHttpMessageHandler Enqueue(
        HttpStatusCode status,
        object? jsonBody = null,
        string? rawText = null,
        IDictionary<string, string>? headers = null)
    {
        _responses.Enqueue(_ =>
        {
            var response = new HttpResponseMessage(status);
            if (headers is not null)
            {
                foreach (var kvp in headers)
                {
                    response.Headers.TryAddWithoutValidation(kvp.Key, kvp.Value);
                }
            }
            if (rawText is not null)
            {
                response.Content = new StringContent(rawText, Encoding.UTF8, "text/plain");
            }
            else if (jsonBody is not null)
            {
                var json = System.Text.Json.JsonSerializer.Serialize(jsonBody);
                response.Content = new StringContent(json, Encoding.UTF8, "application/json");
            }
            return response;
        });
        return this;
    }

    public FakeHttpMessageHandler EnqueueException(Func<HttpRequestMessage, Exception> factory)
    {
        _responses.Enqueue(req => throw factory(req));
        return this;
    }

    public FakeHttpMessageHandler EnqueueDelay(TimeSpan delay)
    {
        _responses.Enqueue(_ =>
        {
            Thread.Sleep(delay);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{}", Encoding.UTF8, "application/json"),
            };
        });
        return this;
    }

    public FakeHttpMessageHandler EnqueueThrowingTaskCanceled()
    {
        _responses.Enqueue(_ =>
        {
            // HttpClient surfaces a slow server as TaskCanceledException
            // with an inner TimeoutException.
            throw new TaskCanceledException("timeout", new TimeoutException("read"));
        });
        return this;
    }

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        Requests.Add(request);
        if (request.Content is not null)
        {
            RequestBodies.Add(await request.Content.ReadAsStringAsync(cancellationToken));
        }
        else
        {
            RequestBodies.Add(null);
        }

        if (_responses.Count == 0)
        {
            return new HttpResponseMessage(HttpStatusCode.InternalServerError)
            {
                Content = new StringContent("no fake response queued", Encoding.UTF8, "text/plain"),
            };
        }

        var factory = _responses.Dequeue();
        return factory(request);
    }
}
