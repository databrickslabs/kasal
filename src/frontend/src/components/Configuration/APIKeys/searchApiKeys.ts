import { ApiKey } from '../../../api';

// Key names are SCREAMING_SNAKE_CASE, so a literal substring test makes the
// obvious query fail: "openai key" never appears in "OPENAI_API_KEY". Separators
// are flattened to spaces and each term is matched independently, so "openai
// key", "kimi" and "powerbi secret" all find their row.
const normalizeForSearch = (value: string): string =>
  value.toLowerCase().replace(/[_\-.]+/g, ' ').replace(/\s+/g, ' ').trim();

export const matchesSearch = (apiKey: ApiKey, query: string): boolean => {
  const terms = normalizeForSearch(query).split(' ').filter(Boolean);
  if (terms.length === 0) return true;
  const haystack = `${normalizeForSearch(apiKey.name)} ${normalizeForSearch(apiKey.description || '')}`;
  return terms.every(term => haystack.includes(term));
};
