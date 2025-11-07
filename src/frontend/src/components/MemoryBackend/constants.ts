/**
 * Constants for Databricks Memory Backend components
 */

// Common embedding models with their dimensions
export const EMBEDDING_MODELS = [
  { 
    name: 'Databricks BGE Large (English)', 
    value: 'databricks-bge-large-en', 
    dimension: 1024,
    description: 'High quality embeddings optimized for English text'
  },
  { 
    name: 'Databricks GTE Large (English)', 
    value: 'databricks-gte-large-en', 
    dimension: 768,
    description: 'General purpose embeddings for English text'
  },
  { 
    name: 'OpenAI text-embedding-3-small', 
    value: 'text-embedding-3-small', 
    dimension: 1536,
    description: 'Cost-effective OpenAI embeddings'
  }
];

// Comprehensive descriptions for each index type
export const INDEX_DESCRIPTIONS = {
  short_term: {
    brief: "Recent agent interactions",
    detailed: "The short-term memory index stores recent conversations and interactions between agents during task execution. It captures immediate context, task progress, decisions made, and intermediate results. This memory is typically retained for the duration of a crew's execution and helps agents maintain context awareness, avoid repeating actions, and build upon previous exchanges. It's essential for coherent multi-agent collaboration within a single session."
  },
  long_term: {
    brief: "Persistent knowledge across sessions",
    detailed: "The long-term memory index preserves important information, learned patterns, and key insights across multiple crew executions. It stores successful strategies, common solutions, domain knowledge, and historical outcomes that agents can reference in future tasks. This persistent memory enables agents to improve over time, apply lessons learned, and maintain institutional knowledge. It's crucial for building intelligent systems that evolve and adapt based on accumulated experience."
  },
  entity: {
    brief: "Information about entities and relationships",
    detailed: "The entity memory index maintains a knowledge graph of entities, their attributes, and relationships discovered during agent operations. It stores information about people, organizations, systems, concepts, and their interconnections. This structured memory helps agents understand context, make informed decisions based on entity relationships, and maintain consistency when referring to specific entities across different tasks. It's vital for complex scenarios involving multiple stakeholders or domain-specific entities."
  },
  document: {
    brief: "Uploaded documents and embeddings",
    detailed: "The document index stores embeddings of uploaded documents, files, and reference materials that agents can search and retrieve during task execution. It includes technical documentation, policies, guidelines, knowledge bases, and any other textual content provided to the system. This semantic search capability allows agents to find relevant information quickly, answer questions based on documented knowledge, and ground their responses in authoritative sources. It's essential for RAG (Retrieval Augmented Generation) workflows."
  }
};