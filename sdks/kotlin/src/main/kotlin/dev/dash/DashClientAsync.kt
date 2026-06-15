package dev.dash

import dev.dash.model.DeleteRequest
import dev.dash.model.DeleteResponse
import dev.dash.model.EmbedRequest
import dev.dash.model.EmbeddingResponse
import dev.dash.model.HealthResponse
import dev.dash.model.IngestRequest
import dev.dash.model.IngestResponse
import dev.dash.model.RetrievalRequest
import dev.dash.model.RetrievalResponse
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * Coroutine wrapper around the synchronous [DashClient].
 *
 * Every method is a `suspend fun` that hops to [Dispatchers.IO] before
 * delegating to the blocking [DashClient] call. This mirrors the
 * structure of the Python [AsyncClient][dev.dash.python.AsyncClient]
 * and the TypeScript `dash-ts` clients.
 *
 * Example:
 * ```
 * val client = DashClientAsync(DashClient("http://localhost:8080", "sk-live-..."))
 * val resp = client.embed(EmbedRequest.of("hello world"))
 * println(resp.data.first().embedding.take(5))
 * ```
 */
class DashClientAsync(
    private val delegate: DashClient = DashClient("http://localhost:8080", null),
) {

    /**
     * Call `POST /v1/embeddings` and return a typed response.
     *
     * @throws DashException on non-2xx responses.
     * @throws DashConnectionException on network failures.
     */
    suspend fun embed(req: EmbedRequest): EmbeddingResponse =
        withContext(Dispatchers.IO) { delegate.embed(req) }

    /**
     * Call `POST /v1/ingest` and return the per-bundle response.
     */
    suspend fun ingest(req: IngestRequest): IngestResponse =
        withContext(Dispatchers.IO) { delegate.ingest(req) }

    /**
     * Call `POST /v1/retrieve` and return the structured
     * Claim + Evidence + Contradiction response.
     */
    suspend fun retrieve(req: RetrievalRequest): RetrievalResponse =
        withContext(Dispatchers.IO) { delegate.retrieve(req) }

    /**
     * Call `POST /v1/delete` and return the per-claim delete
     * acknowledgment.
     */
    suspend fun delete(req: DeleteRequest): DeleteResponse =
        withContext(Dispatchers.IO) { delegate.delete(req) }

    /**
     * Call `GET /health` and return the liveness response.
     */
    suspend fun health(): HealthResponse =
        withContext(Dispatchers.IO) { delegate.health() }

    /** Expose the underlying synchronous [DashClient] for fluent config. */
    fun sync(): DashClient = delegate
}
