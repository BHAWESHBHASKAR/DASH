//! GPU-accelerated vector similarity backend.
//!
//! This module is entirely gated behind the `gpu-backend` cargo
//! feature. The build script wires it into the store at compile
//! time; if the feature is disabled, the store falls back to
//! the CPU similarity implementation in lib.rs.

use std::sync::OnceLock;

#[cfg(feature = "gpu-backend")]
struct GpuBackendEngine {
    device: wgpu::Device,
    queue: wgpu::Queue,
    pipeline: wgpu::ComputePipeline,
    bind_group_layout: wgpu::BindGroupLayout,
}

#[cfg(feature = "gpu-backend")]
static GPU_BACKEND_ENGINE: OnceLock<Result<GpuBackendEngine, String>> = OnceLock::new();

#[cfg(feature = "gpu-backend")]
fn gpu_backend_engine() -> Option<&'static GpuBackendEngine> {
    GPU_BACKEND_ENGINE
        .get_or_init(GpuBackendEngine::new)
        .as_ref()
        .ok()
}

#[cfg(feature = "gpu-backend")]
fn gpu_score_query_candidate_vectors(
    query_vector: &[f32],
    candidate_vectors: &[(String, &[f32])],
) -> Option<Vec<(String, f32)>> {
    let engine = gpu_backend_engine()?;
    engine.score(query_vector, candidate_vectors).ok()
}

#[cfg(feature = "gpu-backend")]
impl GpuBackendEngine {
    fn new() -> Result<Self, String> {
        use wgpu::util::DeviceExt;

        let instance = wgpu::Instance::default();
        let adapter = pollster::block_on(instance.request_adapter(&wgpu::RequestAdapterOptions {
            power_preference: wgpu::PowerPreference::HighPerformance,
            compatible_surface: None,
            force_fallback_adapter: false,
        }))
        .ok_or_else(|| "no GPU adapter available".to_string())?;

        let (device, queue) = pollster::block_on(adapter.request_device(
            &wgpu::DeviceDescriptor {
                label: Some("dash-vector-gpu-device"),
                required_features: wgpu::Features::empty(),
                required_limits: wgpu::Limits::downlevel_defaults(),
            },
            None,
        ))
        .map_err(|err| format!("request_device failed: {err}"))?;

        let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("dash-vector-gpu-cosine-shader"),
            source: wgpu::ShaderSource::Wgsl(GPU_COSINE_SHADER.into()),
        });

        let bind_group_layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("dash-vector-gpu-bind-group-layout"),
            entries: &[
                wgpu::BindGroupLayoutEntry {
                    binding: 0,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 1,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 2,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: false },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 3,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Uniform,
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
            ],
        });

        let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("dash-vector-gpu-pipeline-layout"),
            bind_group_layouts: &[&bind_group_layout],
            push_constant_ranges: &[],
        });

        let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("dash-vector-gpu-cosine-pipeline"),
            layout: Some(&pipeline_layout),
            module: &shader,
            entry_point: "main",
        });

        let _ = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("dash-vector-gpu-init-probe"),
            contents: bytemuck::cast_slice(&[0.0f32]),
            usage: wgpu::BufferUsages::STORAGE,
        });

        Ok(Self {
            device,
            queue,
            pipeline,
            bind_group_layout,
        })
    }

    fn score(
        &self,
        query_vector: &[f32],
        candidate_vectors: &[(String, &[f32])],
    ) -> Result<Vec<(String, f32)>, String> {
        use wgpu::util::DeviceExt;

        if query_vector.is_empty() || candidate_vectors.is_empty() {
            return Ok(Vec::new());
        }

        let mut candidate_ids = Vec::new();
        let mut flattened_candidates = Vec::new();
        for (claim_id, vector) in candidate_vectors {
            if vector.len() != query_vector.len() {
                continue;
            }
            candidate_ids.push(claim_id.clone());
            flattened_candidates.extend_from_slice(vector);
        }

        if candidate_ids.is_empty() {
            return Ok(Vec::new());
        }

        let query_buffer = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("dash-vector-gpu-query-buffer"),
                contents: bytemuck::cast_slice(query_vector),
                usage: wgpu::BufferUsages::STORAGE,
            });
        let candidate_buffer = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("dash-vector-gpu-candidate-buffer"),
                contents: bytemuck::cast_slice(flattened_candidates.as_slice()),
                usage: wgpu::BufferUsages::STORAGE,
            });

        let output_size = (candidate_ids.len() * std::mem::size_of::<f32>()) as wgpu::BufferAddress;
        let output_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("dash-vector-gpu-output-buffer"),
            size: output_size,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
            mapped_at_creation: false,
        });
        let readback_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("dash-vector-gpu-readback-buffer"),
            size: output_size,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        let params = GpuCosineParams {
            dimension: query_vector.len() as u32,
            candidate_count: candidate_ids.len() as u32,
            pad0: 0,
            pad1: 0,
        };
        let params_buffer = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("dash-vector-gpu-params-buffer"),
                contents: bytemuck::bytes_of(&params),
                usage: wgpu::BufferUsages::UNIFORM,
            });

        let bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("dash-vector-gpu-bind-group"),
            layout: &self.bind_group_layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: query_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: candidate_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: output_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 3,
                    resource: params_buffer.as_entire_binding(),
                },
            ],
        });

        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("dash-vector-gpu-command-encoder"),
            });
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("dash-vector-gpu-compute-pass"),
                timestamp_writes: None,
            });
            pass.set_pipeline(&self.pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            let workgroups = ((candidate_ids.len() as u32).saturating_add(63)) / 64;
            pass.dispatch_workgroups(workgroups.max(1), 1, 1);
        }
        encoder.copy_buffer_to_buffer(&output_buffer, 0, &readback_buffer, 0, output_size);
        self.queue.submit(Some(encoder.finish()));

        let readback_slice = readback_buffer.slice(..);
        let (tx, rx) = std::sync::mpsc::channel();
        readback_slice.map_async(wgpu::MapMode::Read, move |result| {
            let _ = tx.send(result);
        });
        self.device.poll(wgpu::Maintain::Wait);
        rx.recv()
            .map_err(|_| "gpu map_async channel dropped".to_string())?
            .map_err(|err| format!("gpu map_async failed: {err}"))?;

        let bytes = readback_slice.get_mapped_range();
        let scores: Vec<f32> = bytemuck::cast_slice(&bytes).to_vec();
        drop(bytes);
        readback_buffer.unmap();

        Ok(candidate_ids.into_iter().zip(scores).collect())
    }
}
