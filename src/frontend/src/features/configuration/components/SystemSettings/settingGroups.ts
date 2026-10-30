/**
 * Labels for the server-wide settings (Configuration → Engines settings
 * registry, `engine_settings.SETTINGS` in the backend). Grouped by the section
 * a setting belongs to; each section shows its own group in an Advanced panel.
 * Every one of these replaced an environment variable a Databricks App never sets.
 */
export interface FieldMeta {
  label: string;
  helper: string;
  decimal?: boolean;
}

export interface SettingGroup {
  title: string;
  fields: Record<string, FieldMeta>;
}

export type SettingGroupId = 'memory' | 'knowledge' | 'chat' | 'a2ui' | 'tools' | 'triggers' | 'recipes' | 'models';

export const SETTING_GROUPS: Record<SettingGroupId, SettingGroup> = {
  models: {
    title: 'Model fallback',
    fields: {
      fallback_model: {
        label: 'Fallback model key',
        helper: 'Used instead of a Databricks model when no Databricks workspace is available. Must be an enabled model; empty ranks the enabled ones.',
      },
    },
  },
  memory: {
    title: 'Memory maintenance',
    fields: {
      memory_sweep_enabled: { label: 'Background memory sweep', helper: '' },
      memory_sweep_interval_hours: {
        label: 'Sweep a workspace every (hours)',
        helper: 'How long a workspace waits between background maintenance passes.',
        decimal: true,
      },
      memory_sweep_batch: { label: 'Workspaces per sweep', helper: 'Each costs up to two LLM calls; the rest wait for the next tick.' },
      memory_maintenance_interval: {
        label: 'Seconds between passes on one scope',
        helper: 'Throttles the pass that follows a run. 0 runs it after every run.',
      },
    },
  },
  knowledge: {
    title: 'Knowledge search',
    fields: {
      knowledge_min_score: { label: 'Minimum relevance', helper: 'Similarity a chunk must reach to be shown to an agent.', decimal: true },
      knowledge_max_searches: { label: 'Searches per agent turn', helper: 'Stops an agent that keeps rephrasing the same search. 0 = unlimited.' },
      knowledge_ttl_days: { label: 'Keep uploads (days)', helper: 'Uploaded documents are removed after this long. 0 keeps them.' },
    },
  },
  chat: {
    title: 'Chat',
    fields: {
      chat_token_streaming: { label: 'Stream chat answers as they are written', helper: '' },
      crew_token_streaming: { label: 'Stream crew answers as they are written', helper: '' },
      chat_compaction: { label: 'Summarise long conversations', helper: '' },
      chat_compaction_keep_rows: { label: 'Messages kept verbatim', helper: 'The newest messages stay out of the summary.' },
      chat_compaction_trigger_chars: { label: 'Summarise after (characters)', helper: 'Older turns are folded into the summary past this size.' },
      chat_summary_max_chars: { label: 'Summary length (characters)', helper: 'Ceiling on the running conversation summary.' },
      chat_history_recent_limit: { label: 'History messages read', helper: 'How many recent messages the conversation context considers.' },
      chat_history_user_char_cap: { label: 'Characters per user message', helper: 'Each earlier user message is cut to this length.' },
      chat_history_assistant_char_cap: { label: 'Characters per earlier answer', helper: 'Older answers are kept as short stubs.' },
      chat_history_last_answer_char_cap: { label: 'Characters of the answer on screen', helper: 'The latest answer is kept (almost) whole so it can be restated.' },
      chat_history_max_assistant_turns: { label: 'Earlier answers included', helper: 'How many earlier answers appear in the context.' },
      chat_history_max_chars: { label: 'History budget (characters)', helper: 'Total size of the conversation context.' },
      chat_memory_settle_seconds: {
        label: 'Remember an answer after (seconds)',
        helper: 'A streamed answer is saved to memory once it stops changing for this long.',
        decimal: true,
      },
    },
  },
  a2ui: {
    title: 'Rich answers (A2UI)',
    fields: {
      a2ui_enabled: { label: 'Compose rich answer surfaces', helper: '' },
      a2ui_early: { label: 'Start the layout before the answer finishes', helper: '' },
      a2ui_streaming: { label: 'Stream surfaces as they are composed', helper: '' },
      a2ui_stream_interval_ms: { label: 'Stream update interval (ms)', helper: 'How often a streaming surface is refreshed.' },
      a2ui_compose_retries: { label: 'Compose attempts', helper: 'Tries before falling back to plain text. Raise it for weaker models.' },
      a2ui_compose_timeout: {
        label: 'Compose timeout (seconds)',
        helper: 'How long a chat answer waits for its surface; the text is already shown.',
        decimal: true,
      },
    },
  },
  tools: {
    title: 'Tool limits',
    fields: {
      scrape_max_chars: { label: 'Website text returned (characters)', helper: 'A scraped page goes straight into the conversation.' },
      scrape_max_fetch_bytes: { label: 'Website bytes fetched', helper: 'Download ceiling before any text is extracted.' },
      dax_llm_batch_size: { label: 'DAX measures per LLM call', helper: 'Measures translated together by the DAX fallback.' },
      embedding_batch_size: { label: 'Texts per embedding request', helper: 'Batch size for the Databricks embedding endpoint.' },
      embedding_timeout_seconds: { label: 'Embedding batch timeout (seconds)', helper: 'Batch and local (Ollama) embedding requests.', decimal: true },
      embedding_request_timeout_seconds: { label: 'Embedding request timeout (seconds)', helper: 'A single-text embedding request.', decimal: true },
    },
  },
  triggers: {
    title: 'Event trigger queue',
    fields: {
      event_triggers_interval: { label: 'Check the queue every (seconds)', helper: 'How often queued events are dispatched.' },
      event_triggers_batch: { label: 'Events per check', helper: 'Queued events dispatched per tick.' },
      event_triggers_max_hops: { label: 'Chain depth limit', helper: 'A run this many hops into a chain emits nothing, which stops cycles.' },
    },
  },
  recipes: {
    title: 'Workflow recipes',
    fields: {
      workflow_recipe_exemplars: { label: 'Use curated past crews as examples', helper: '' },
      workflow_recipe_min_similarity: { label: 'Minimum similarity', helper: 'How close a past crew must be to be offered.', decimal: true },
      workflow_recipe_holdout: {
        label: 'Holdout fraction',
        helper: 'Share of eligible generations denied examples, to measure the effect. 0 = off.',
        decimal: true,
      },
      workflow_recipe_mine_batch: { label: 'Runs mined per pass', helper: 'Completed crew runs distilled into recipes at a time.' },
    },
  },
};
