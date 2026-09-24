"""On-demand advice only: never changes a run's explicit model or effort."""

from src.core.llm.effort import EFFORT_PROFILES
from src.schemas.decision_config import DecisionRecommendationResponse
from src.services.decisions.policies import question
from src.services.decisions.runtime import decide
from src.services.settings.models import ModelConfigService
from src.utils.model_config import model_supports_reasoning_effort


async def recommend(session, group_context, prompt):
    models = await ModelConfigService(
        session, group_context.primary_group_id
    ).find_enabled_models_for_group(group_context)
    if not models or len(models) > 64:
        return DecisionRecommendationResponse()
    candidates = [
        {
            "name": m.name,
            "provider": m.provider,
            "context_window": m.context_window,
            "max_output_tokens": m.max_output_tokens,
            "supports_reasoning_effort": model_supports_reasoning_effort(m.key),
        }
        for m in models
    ]
    answers = await decide(
        "model_effort_recommendation",
        {
            "request": prompt,
            "models": candidates,
            "effort_profiles": EFFORT_PROFILES,
        },
        {
            "model": question(
                "Recommend a model from the enabled candidates using only supplied capabilities. "
                "Do not invent pricing or benchmark claims. Abstain if the metadata cannot distinguish suitability.",
                {
                    **{str(i): f"Model {i}" for i in range(len(models))},
                    "none": "Insufficient evidence to recommend a model",
                },
            ),
            "effort": question(
                "Choose a sufficient execution allowance for the request using the supplied profiles.",
                {key: f"Execution allowance {key}" for key in EFFORT_PROFILES},
            ),
        },
        group_id=group_context.primary_group_id,
    )
    if answers is None:
        return DecisionRecommendationResponse()
    selected = answers["model"].selected
    return DecisionRecommendationResponse(
        model=models[int(selected)].key if selected != "none" else None,
        effort=answers["effort"].selected,
    )
