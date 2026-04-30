from __future__ import annotations

import json
import os
import urllib.request
from typing import Any


ORDER = {"賣出": 0, "持有": 1, "買入": 2}


def _provider_config(provider: str | None, model: str | None) -> tuple[str | None, str | None, str]:
    normalized = (provider or "openai").lower()
    if normalized == "openrouter":
        return os.getenv("OPENROUTER_API_KEY"), "https://openrouter.ai/api/v1/chat/completions", model or "openai/gpt-4o-mini"
    if normalized == "custom":
        return os.getenv("CUSTOM_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY"), os.getenv("CUSTOM_OPENAI_BASE_URL"), model or "gpt-4o-mini"
    if normalized == "local":
        return os.getenv("LOCAL_OPENAI_API_KEY", "local"), os.getenv("LOCAL_OPENAI_BASE_URL"), model or "local-model"
    return os.getenv("OPENAI_API_KEY"), "https://api.openai.com/v1/chat/completions", model or "gpt-4o-mini"


def _post_json(url: str, api_key: str, payload: dict[str, Any], timeout: float = 45.0) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _extract_json(text: str) -> dict[str, Any]:
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("LLM response did not contain a JSON object")
    return json.loads(text[start : end + 1])


def apply_llm_review(
    recommendation: dict[str, Any],
    evidence_pack: dict[str, Any],
    provider: str | None,
    model: str | None,
) -> dict[str, Any]:
    api_key, url, resolved_model = _provider_config(provider, model)
    reviewed = dict(recommendation)
    reviewed["recommendation_source"] = "deterministic_plus_llm"
    if not api_key or not url:
        reviewed["llm_review"] = {"enabled": True, "provider": provider, "model": resolved_model, "status": "fallback:no-api-config"}
        return reviewed

    prompt = {
        "task": "Review the deterministic Taiwan sector candidate recommendation. Return JSON only. You may downgrade the recommendation, add risk notes, or keep it. Do not invent facts outside evidence_pack.",
        "allowed_recommendations": ["買入", "持有", "賣出"],
        "hard_rules": [
            "Do not change rank or scores.",
            "Do not upgrade the deterministic recommendation.",
            "If evidence is insufficient, choose 持有.",
        ],
        "deterministic_recommendation": recommendation,
        "evidence_pack": evidence_pack,
        "required_json_keys": ["recommendation", "bull_case", "bear_case", "risk_review", "manager_decision"],
    }
    try:
        response = _post_json(
            url,
            api_key,
            {
                "model": resolved_model,
                "temperature": 0.1,
                "messages": [
                    {"role": "system", "content": "You are a conservative Taiwan equity sector research risk reviewer. Output JSON only."},
                    {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
                ],
            },
        )
        content = response["choices"][0]["message"]["content"]
        payload = _extract_json(content)
        proposed = str(payload.get("recommendation") or recommendation.get("recommendation"))
        if proposed not in ORDER:
            proposed = "持有"
        original = str(recommendation.get("recommendation") or "持有")
        if ORDER[proposed] > ORDER.get(original, 1):
            proposed = original
        reviewed["recommendation"] = proposed
        notes = dict(reviewed.get("review_notes") or {})
        for key in ["bull_case", "bear_case", "risk_review", "manager_decision"]:
            if payload.get(key):
                notes[key] = str(payload[key])
        reviewed["review_notes"] = notes
        reviewed["llm_review"] = {"enabled": True, "provider": provider, "model": resolved_model, "status": "ok"}
        return reviewed
    except Exception as exc:
        reviewed["llm_review"] = {
            "enabled": True,
            "provider": provider,
            "model": resolved_model,
            "status": "fallback:error",
            "error": str(exc),
        }
        return reviewed
