import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  CrewLabels,
  PromptOptimizationService,
} from '../../../../api/config/PromptOptimizationService';

export interface CrewLabelsState {
  /** Expected facts, one per line. */
  facts: string;
  setFacts: (value: string) => void;
  answer: string;
  setAnswer: (value: string) => void;
  /** Review notes offered as labels; none is used until accepted. */
  suggestions: string[];
  /** Add a suggestion (or the user's edit of it) to the expected facts. */
  accept: (suggestion: string, text?: string) => void;
  /** Label fields this run has, for the judges that need labels. */
  present: string[];
  /** The labels a run starts with, or undefined when there is nothing to send. */
  payload: () => CrewLabels | undefined;
  /** Why selected judges were left out of the last run started. */
  skipped: string[];
  setSkipped: (reasons: string[]) => void;
}

/** One fact per non-blank line; a leading "-" or "*" bullet is dropped. */
export const parseFacts = (text: string): string[] =>
  text
    .split('\n')
    .map((line) => line.trim().replace(/^[-*]\s*/, '').trim())
    .filter(Boolean);

/**
 * The crew's labels in the Optimize dialog: the confirmed ones are loaded into
 * the fields, and review notes are offered as suggestions that only become
 * labels when the user accepts or edits one. The run's request carries what
 * the fields hold, and the server saves it for the crew when it changed.
 */
export const useCrewLabels = (open: boolean, crewId: string | null): CrewLabelsState => {
  const [facts, setFacts] = useState('');
  const [answer, setAnswer] = useState('');
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [hasReviewNotes, setHasReviewNotes] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const [skipped, setSkipped] = useState<string[]>([]);

  useEffect(() => {
    if (!open || !crewId) return;
    let cancelled = false;
    setLoaded(false);
    PromptOptimizationService.getCrewLabels(crewId)
      .then((info) => {
        if (cancelled) return;
        setFacts((info.labels?.expected_facts || []).join('\n'));
        setAnswer(info.labels?.expected_response || '');
        setSuggestions(info.suggestions || []);
        setHasReviewNotes(Boolean(info.has_review_notes));
        setLoaded(true);
      })
      .catch(() => {
        // Labels are optional: without them the label judges stay disabled.
        if (!cancelled) setSuggestions([]);
      });
    return () => {
      cancelled = true;
    };
  }, [open, crewId]);

  const accept = useCallback((suggestion: string, text: string = suggestion) => {
    const added = parseFacts(text);
    if (added.length) {
      setFacts((prev) => [...parseFacts(prev), ...added].join('\n'));
    }
    setSuggestions((prev) => prev.filter((s) => s !== suggestion));
  }, []);

  const present = useMemo(() => {
    const fields: string[] = [];
    if (parseFacts(facts).length) fields.push('expected_facts');
    if (answer.trim()) fields.push('expected_response');
    if (hasReviewNotes) fields.push('guidelines');
    return fields;
  }, [facts, answer, hasReviewNotes]);

  const payload = useCallback((): CrewLabels | undefined => {
    const labels = { expected_facts: parseFacts(facts), expected_response: answer.trim() };
    // Sent when the saved labels were read (so clearing them is saved too),
    // or when the user typed some; never an empty set over unread labels.
    if (!loaded && !labels.expected_facts.length && !labels.expected_response) return undefined;
    return labels;
  }, [facts, answer, loaded]);

  return {
    facts,
    setFacts,
    answer,
    setAnswer,
    suggestions,
    accept,
    present,
    payload,
    skipped,
    setSkipped,
  };
};
