// Templates with optimization wiring (training-data source + scorers) in the
// backend's TEMPLATE_TASKS registry. Other seeded templates appear here as
// their wiring is added. Shared by the Instructions list (per-template
// Optimize action) and the Optimization panel (template picker).
export const OPTIMIZABLE_TEMPLATES = [
  { name: 'detect_intent', label: 'detect_intent — chat/canvas intent routing' },
  { name: 'generate_agent', label: 'generate_agent — agent generation' },
  { name: 'generate_task', label: 'generate_task — task generation' },
  { name: 'generate_crew', label: 'generate_crew — full crew generation' },
  { name: 'generate_crew_plan', label: 'generate_crew_plan — crew plan outline' },
  { name: 'generate_job_name', label: 'generate_job_name — run naming' },
];
