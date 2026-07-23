/**
 * Constants for Databricks Memory Backend components
 */

// Common embedding models with their dimensions
export const EMBEDDING_MODELS = [
  {
    name: 'Databricks GTE Large (English)',
    value: 'databricks-gte-large-en',
    dimension: 1024,
    description: 'Best Databricks native embedding model (8192 token context)'
  },
  {
    name: 'Databricks BGE Large (English)',
    value: 'databricks-bge-large-en',
    dimension: 1024,
    description: 'Normalized embeddings for English text (512 token context)'
  },
  {
    name: 'OpenAI text-embedding-3-large',
    value: 'text-embedding-3-large',
    dimension: 3072,
    description: 'High quality OpenAI embeddings'
  }
];

// Comprehensive descriptions for each index type (CrewAI 1.10+ unified memory)
export const INDEX_DESCRIPTIONS = {
  memory: {
    brief: "Unified cognitive memory (all records)",
    detailed: "CrewAI 1.10+ replaces the old short-term / long-term / entity split with a single unified memory class. Every record lives in this one index, tagged with a hierarchical scope path (e.g. /crew/research/findings) and category tags. Recall blends semantic similarity, recency, and LLM-inferred importance into a composite score; save-time consolidation automatically merges contradictions. Short-term-style session scoping is preserved via the session_id column."
  },
  document: {
    brief: "Uploaded documents and embeddings",
    detailed: "The document index stores embeddings of uploaded documents, files, and reference materials that agents can search and retrieve during task execution. It includes technical documentation, policies, guidelines, knowledge bases, and any other textual content provided to the system. This semantic search capability allows agents to find relevant information quickly, answer questions based on documented knowledge, and ground their responses in authoritative sources. It's essential for RAG (Retrieval Augmented Generation) workflows."
  }
};