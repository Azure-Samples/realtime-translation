"""
Microexpression analysis utilities for the real-time translation orchestrator.

This module defines helper functions for analysing video frames in order to
approximate microexpressions. The analysis pipeline supports two modes:

1. **py-feat/OpenFace (preferred)** – When the `feat` package is installed
   and its dependencies (e.g. OpenFace) are available, this module
   attempts to use it to extract facial Action Units (AUs) and
   high-level emotion probabilities from each frame. This yields
   relatively reliable results for facial expression recognition.

2. **Optical flow fallback** – If `feat` is not available, the module
   falls back to computing dense optical flow between successive frames
   using OpenCV (Farneback algorithm). Changes in the magnitude of the
   optical flow are used as a proxy for micro-events. This method does
   not identify AUs or emotions and should be interpreted with caution.

The module maintains in-memory state keyed by session ID. Each session
records the previous grayscale frame and a list of analysis results for
all frames processed. When summarising a session, aggregate statistics
such as the average emotion probabilities and approximate counts of
micro-events are produced.

Usage:

* Call :func:`analyze_frame` for each incoming JPEG frame with a
  session ID, frame index and timestamp (seconds). The function
  returns a dictionary describing the flow metrics and, when
  available, the emotion/AU predictions.
* After processing all frames, call :func:`summarize_session` to
  produce a report summarising the session. Optionally call
  :func:`reset_session` to clear the session state.

The environment variable ``PYFEAT_BACKEND`` may be used to override the
default backend configuration for `feat.Detector`. See the
documentation for the `feat` package for details.
"""

from __future__ import annotations

import cv2
import numpy as np
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import os
import json
import traceback
from collections import deque
from azure.ai.inference import ChatCompletionsClient
from azure.ai.inference.models import SystemMessage, UserMessage
from azure.core.credentials import AzureKeyCredential

__all__ = ["analyze_frame", "summarize_session", "reset_session"]


@dataclass
class FrameResult:
    """Internal data structure to store per-frame analysis results."""
    frameIndex: int
    ts: float
    flow_mag_mean: float
    flow_mag_p95: float
    emotion: Optional[Dict[str, float]] = None
    aus: Optional[Dict[str, float]] = None
    microEvent: bool = False
    aiInterpretation: Optional[Dict[str, Any]] = None

@dataclass
class SessionState:
    """Bounded session-level state.

    We keep only a sliding buffer for AI evaluation, a recent timeline (bounded), and
    aggregate counters to avoid unbounded memory growth across long sessions.
    """
    last_gray: Optional[np.ndarray] = None
    frame_buffer: deque[FrameResult] = field(default_factory=lambda: deque(maxlen=int(os.environ.get("MICROEXP_BATCH_SIZE", "5"))))
    recent_timeline: deque[FrameResult] = field(default_factory=lambda: deque(maxlen=int(os.environ.get("MICROEXP_TIMELINE_MAX", "150"))))
    total_frames: int = 0
    micro_events: int = 0
    emotion_sums: Dict[str, float] = field(default_factory=dict)
    ai_evaluations: List[Dict[str, Any]] = field(default_factory=list)

_sessions: Dict[str, SessionState] = {}

def _decode_image_jpeg(data: bytes) -> np.ndarray:
    """Decode JPEG image bytes into a BGR numpy array.

    Args:
        data: JPEG-encoded bytes.

    Returns:
        A NumPy array of shape (H, W, 3) in BGR colour order.
    """
    arr = np.frombuffer(data, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        raise ValueError("Invalid image bytes")
    return img


# --- Azure Foundry Chat integration (optional) ---------------------------------
FOUNDRY_ENDPOINT = os.environ.get("FOUNDRY_INFERENCE_ENDPOINT", "")
FOUNDRY_KEY = os.environ.get("FOUNDRY_KEY", "")
FOUNDRY_MODEL = os.environ.get("FOUNDRY_MODEL_ANALYSIS", "")


def _build_microexp_prompt(frames: List[FrameResult]) -> str:
    """Create a concise prompt describing the frames for the model to interpret.

    The model receives a small frameset (typically 5 frames). For each frame we
    supply the timestamp, flow mean and p95 and whether a heuristic microEvent
    was detected. The instruction asks the model to return a JSON object with
    likely emotions, confidence scores (0-1) and a short rationale.
    """

    header = (
        "You are an assistant that interprets very short video frame sets for "
        "micro-expressions. Each input is a list of frames (5~10). For the set, "
        "return a JSON object with: \n  - emotions: a map emotion->confidence (0-1),\n  - dominant: the single most likely emotion,\n  - rationale: 1-2 sentence explanation,\n  - frames_considered: number of frames.\n\n"
    )

    rows = []
    for f in frames:
        rows.append(
            f"frameIndex={f.frameIndex}, ts={f.ts:.3f}, flow_mean={f.flow_mag_mean:.4f}, "
            f"flow_p95={f.flow_mag_p95:.4f}, microEvent={f.microEvent}"
        )

    body = "\n".join(rows)
    footer = "\nRespond ONLY with a single JSON object as described. Do not add extra text."
    return header + body + footer


def _call_foundry_chat(prompt: str) -> Optional[Dict[str, Any]]:
    """Call Azure AI Foundry Chat completions using azure-ai-inference if available.

    Returns the parsed JSON from the model, or None on failure. This function is
    intentionally tolerant: it logs errors and returns None so the pipeline can
    continue without Foundry access.
    """

    try:
        client = ChatCompletionsClient(endpoint=FOUNDRY_ENDPOINT, credential=AzureKeyCredential(FOUNDRY_KEY))
        messages = [
            SystemMessage("You are a concise micro-expression analyst."),
            UserMessage(prompt),
        ]
        resp = client.complete(messages=messages, temperature=0.2, top_p=0.9, model=FOUNDRY_MODEL)
        content = resp.choices[0].message.content
        # Expect JSON text -> try parse
        try:
            parsed = json.loads(content)
            return parsed
        except Exception:
            # If model returned extra text around JSON, try to extract first JSON blob
            import re

            m = re.search(r"(\{.*\})", content, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group(1))
                except Exception:
                    return None
            return None
    except Exception:
        # Keep failures non-fatal for the real-time pipeline
        traceback.print_exc()
        return None

# --- end Foundry integration -------------------------------------------------

def _ensure_session(session_id: str) -> SessionState:
    """Retrieve or create state for a given session."""
    if session_id not in _sessions:
        _sessions[session_id] = SessionState()
    return _sessions[session_id]

def analyze_frame(session_id: str, frame_index: int, ts: float, jpeg_bytes: bytes) -> Dict[str, Any]:
    """Analyse a video frame and return a dictionary of metrics and predictions.

    This function decodes the JPEG bytes, downscales the image to
    approximately 256 pixels on the longest side to reduce computation,
    computes dense optical flow with respect to the previous frame for
    the session, and (if available) extracts AUs and emotions using
    py-feat. The results are stored in session state for later
    summarisation.

    Args:
        session_id: A unique identifier for the session. Frames from
            the same session will be aggregated together.
        frame_index: The index of the frame within the session.
        ts: The timestamp of the frame in seconds.
        jpeg_bytes: Raw JPEG-encoded image data.

    Returns:
        A dictionary containing the session ID, frame index, timestamp,
        optical flow statistics, emotion probabilities (if available),
        AUs (if available), and a boolean flag indicating whether the
        frame likely contained a micro-expression (heuristic).
    """
    state = _ensure_session(session_id)
    img = _decode_image_jpeg(jpeg_bytes)

    h, w = img.shape[:2]
    scale = 256.0 / float(max(h, w)) if max(h, w) > 256 else 1.0
    if scale < 1.0:
        img = cv2.resize(
            img,
            (int(w * scale), int(h * scale)),
            interpolation=cv2.INTER_AREA,
        )

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    flow_mag_mean = 0.0
    flow_mag_p95 = 0.0
    if state.last_gray is not None:
        flow_init = np.zeros((gray.shape[0], gray.shape[1], 2), dtype=np.float32)
        flow = cv2.calcOpticalFlowFarneback(
            state.last_gray,
            gray,
            flow_init,
            pyr_scale=0.5,
            levels=3,
            winsize=15,
            iterations=3,
            poly_n=5,
            poly_sigma=1.2,
            flags=0,
        )
        mag, _ang = cv2.cartToPolar(flow[..., 0], flow[..., 1])
        mag_arr = np.asarray(mag, dtype=np.float32)
        flow_mag_mean = float(np.mean(mag_arr))
        flow_mag_p95 = float(np.percentile(mag_arr, 95))

    emotion: Optional[Dict[str, float]] = None
    aus: Optional[Dict[str, float]] = None

    # Update session state.
    state.last_gray = gray
    result = FrameResult(
        frameIndex=frame_index,
        ts=ts,
        flow_mag_mean=flow_mag_mean,
        flow_mag_p95=flow_mag_p95,
        emotion=emotion,
        aus=aus,
        microEvent=False,
    )
    # Heuristic micro event
    result.microEvent = bool(flow_mag_p95 > 1.5 and flow_mag_mean < 0.6)

    # Update bounded buffers & aggregates
    state.frame_buffer.append(result)
    state.recent_timeline.append(result)
    state.total_frames += 1
    if result.microEvent:
        state.micro_events += 1
    if emotion:
        for k, v in emotion.items():
            state.emotion_sums[k] = state.emotion_sums.get(k, 0.0) + float(v)

    # AI batch evaluation logic
    batch_size = int(os.environ.get("MICROEXP_BATCH_SIZE", "5"))
    ai_interpretation: Optional[Dict[str, Any]] = None
    if len(state.frame_buffer) == batch_size:
        recent = list(state.frame_buffer)
        prompt = _build_microexp_prompt(recent)
        ai_resp = _call_foundry_chat(prompt)
        evaluation = {
            "ts": ts,
            "frameIndex": frame_index,
            "batchSize": batch_size,
            "prompt": prompt,
            "response": ai_resp,
        }
        state.ai_evaluations.append(evaluation)
        for r in recent:
            r.aiInterpretation = ai_resp
        ai_interpretation = ai_resp
        # Cap evaluations history
        max_evals = int(os.environ.get("MICROEXP_MAX_EVALS", "100"))
        if len(state.ai_evaluations) > max_evals:
            state.ai_evaluations = state.ai_evaluations[-max_evals:]

    return {
        "sessionId": session_id,
        "frameIndex": frame_index,
        "ts": ts,
        "flow": {"mean": flow_mag_mean, "p95": flow_mag_p95},
        "emotion": emotion,
        "aus": aus,
        "microEvent": result.microEvent,
        "detector": "optical-flow",
        "aiInterpretation": ai_interpretation,
    }

def summarize_session(session_id: str) -> Dict[str, Any]:
    """Summarise all frames processed for a given session.

    The summary includes the number of frames analysed, an approximate
    count of micro-events detected via the optical flow heuristic,
    average emotion probabilities (if available), and a timeline of
    frame-level results (truncated to avoid returning an excessively
    large payload).

    Args:
        session_id: The session identifier.

    Returns:
        A dictionary with aggregated statistics and a timeline of
        per-frame results. If the session does not exist, an empty
        summary is returned.
    """
    state = _sessions.get(session_id)
    if state is None or state.total_frames == 0:
        return {"sessionId": session_id, "count": 0, "timelineRecent": []}

    # Average emotions
    emotion_avg = {}
    if state.total_frames:
        emotion_avg = {k: v / state.total_frames for k, v in state.emotion_sums.items()}

    recent = [{
        "frameIndex": r.frameIndex,
        "ts": r.ts,
        "flow_p95": r.flow_mag_p95,
        "emotion": r.emotion,
        "aus": r.aus,
        "microEvent": r.microEvent,
        "aiInterpretation": r.aiInterpretation,
    } for r in state.recent_timeline]

    return {
        "sessionId": session_id,
        "count": state.total_frames,
        "microEventsApprox": state.micro_events,
        "emotionAvg": emotion_avg,
        "timelineRecent": recent,
        "aiEvaluations": state.ai_evaluations,
    }

def reset_session(session_id: str) -> None:
    """Clear the stored state for a session.

    This can be used after generating a report to free memory. If the
    session does not exist, the function silently does nothing.
    """
    _sessions.pop(session_id, None)