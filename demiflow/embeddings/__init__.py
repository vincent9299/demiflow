"""Model declarations for Dataset.map_embeddings; inference imports stay lazy."""
from .model import EmbeddingModel
from .config import embedding_execution_config, embedding_options

__all__ = ['EmbeddingModel', 'embedding_execution_config', 'embedding_options']
