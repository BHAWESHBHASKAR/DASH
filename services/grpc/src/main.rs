use embeddings::shared_embedding_provider;
use std::sync::Arc;
use tonic::{Request, Response, Status, transport::Server};

pub mod dash {
    include!("dash.v1.rs");
}

use dash::{
    EmbedRequest, EmbedResponse, EmbeddingData as GrpcEmbeddingData, ListModelsRequest,
    ListModelsResponse, ModelData as GrpcModelData, Usage as GrpcUsage,
    dash_server::{Dash, DashServer},
};

pub struct DashService {
    provider: Arc<dyn embeddings::EmbeddingProvider + Send + Sync + 'static>,
}

impl DashService {
    fn new(provider: Arc<dyn embeddings::EmbeddingProvider + Send + Sync + 'static>) -> Self {
        Self { provider }
    }
}

#[tonic::async_trait]
impl Dash for DashService {
    async fn embed(
        &self,
        request: Request<EmbedRequest>,
    ) -> Result<Response<EmbedResponse>, Status> {
        let req = request.into_inner();
        let texts: Vec<String> = req.input.into_iter().collect();
        let vectors = self
            .provider
            .embed(&texts)
            .map_err(|e| Status::internal(format!("embedding failed: {e}")))?;

        let mut data = Vec::with_capacity(vectors.len());
        let mut total_tokens = 0i32;
        for (index, vector) in vectors.into_iter().enumerate() {
            total_tokens += vector.len() as i32;
            let embedding: Vec<f32> = if req.normalize {
                let norm = vector.iter().map(|v| v * v).sum::<f32>().sqrt();
                if norm > 0.0 {
                    vector.iter().map(|v| v / norm).collect()
                } else {
                    vector
                }
            } else {
                vector
            };
            data.push(GrpcEmbeddingData {
                object: "embedding".to_string(),
                index: index as i32,
                embedding,
            });
        }

        let model = if req.model.is_empty() {
            embeddings::embedding_provider_name_from_env()
        } else {
            req.model
        };

        Ok(Response::new(EmbedResponse {
            object: "list".to_string(),
            data,
            model,
            usage: Some(GrpcUsage {
                prompt_tokens: total_tokens,
                total_tokens,
            }),
        }))
    }

    async fn list_models(
        &self,
        _request: Request<ListModelsRequest>,
    ) -> Result<Response<ListModelsResponse>, Status> {
        let models = vec![
            GrpcModelData {
                id: "dash-hash".to_string(),
                object: "model".to_string(),
                created: 0,
                owned_by: "dash".to_string(),
            },
            GrpcModelData {
                id: "nomic-embed-text".to_string(),
                object: "model".to_string(),
                created: 0,
                owned_by: "ollama".to_string(),
            },
            GrpcModelData {
                id: "text-embedding-3-small".to_string(),
                object: "model".to_string(),
                created: 0,
                owned_by: "openai".to_string(),
            },
            GrpcModelData {
                id: "text-embedding-3-large".to_string(),
                object: "model".to_string(),
                created: 0,
                owned_by: "openai".to_string(),
            },
        ];
        Ok(Response::new(ListModelsResponse {
            object: "list".to_string(),
            data: models,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn list_models_returns_known_models() {
        let provider = embeddings::shared_embedding_provider();
        let service = DashService::new(provider);
        let request = tonic::Request::new(ListModelsRequest {});
        let response = service.list_models(request).await.unwrap();
        let ids: Vec<_> = response
            .get_ref()
            .data
            .iter()
            .map(|m| m.id.as_str())
            .collect();
        assert!(ids.contains(&"dash-hash"));
        assert!(ids.contains(&"nomic-embed-text"));
    }

    #[tokio::test]
    async fn embed_returns_one_vector_per_input() {
        let provider = embeddings::shared_embedding_provider();
        let service = DashService::new(provider);
        let request = tonic::Request::new(EmbedRequest {
            model: "dash-hash".to_string(),
            input: vec!["first sentence".to_string(), "second sentence".to_string()],
            normalize: false,
        });
        let response = service.embed(request).await.unwrap();
        assert_eq!(response.get_ref().data.len(), 2);
        assert!(!response.get_ref().data[0].embedding.is_empty());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dash_common::init_logging();
    let bind = std::env::var("DASH_GRPC_BIND").unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    let provider = shared_embedding_provider();
    let service = DashService::new(provider);

    tracing::info!("dash grpc server listening on {}", bind);
    Server::builder()
        .add_service(DashServer::new(service))
        .serve(bind.parse()?)
        .await?;

    Ok(())
}
