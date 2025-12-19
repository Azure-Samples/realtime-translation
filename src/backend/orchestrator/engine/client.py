"""
Client and command abstractions for Azure OpenAI real-time translation.

This module defines the AOAITranslationClient base class, the TranslateCommand for translation logic, and the Invoker context manager for orchestrating translation sessions and audio forwarding between ACS and Azure OpenAI.
"""

import os
import time
import json
import base64
import asyncio

from abc import ABC, abstractmethod
from typing import Optional, Protocol, runtime_checkable, Any

from numpy import ndarray

from fastapi import WebSocket

from semantic_kernel import Kernel
from semantic_kernel.connectors.ai import FunctionChoiceBehavior
from semantic_kernel.contents import AudioContent, RealtimeAudioEvent
from semantic_kernel.connectors.ai.open_ai import AzureRealtimeExecutionSettings, AzureRealtimeWebsocket
from semantic_kernel.connectors.ai.open_ai.services._open_ai_realtime import ListenEvents

import jinja2

from orchestrator import logger

from orchestrator.storage.cosmos import CosmosCRUD


TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "prompts")
JINJA_ENV = jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATES_DIR))

FOUNDRY_KEY=os.getenv("FOUNDRY_KEY", "")
FOUNDRY_MODEL_VERSION=os.getenv("FOUNDRY_MODEL_VERSION", "2025-08-28")
FOUNDRY_ENDPOINT=os.getenv("FOUNDRY_INFERENCE_ENDPOINT", "")
FOUNDRY_MODEL_ANALYSIS=os.getenv("FOUNDRY_MODEL_ANALYSIS", "")
FOUNDRY_MODEL_REALTIME=os.getenv("FOUNDRY_MODEL_REALTIME", "")


class AOAITranslationClient(ABC):
    """Abstract base for Azure OpenAI real-time translation clients.

    Manages configuration, environment validation, and exposes the
    underlying AzureRealtimeWebsocket as an async context manager.
    """

    def __init__(self, observer=None):
        """
        Args:
            ws (WebSocket): The FastAPI WebSocket connection.
            observer (Optional[Any]): Optional observer/handler for events.
        """
        self.kernel = Kernel()
        self._raw_ws = AzureRealtimeWebsocket(
                api_key=FOUNDRY_KEY,
                api_version=FOUNDRY_MODEL_VERSION,
                azure_endpoint=FOUNDRY_ENDPOINT,
                deployment_name=FOUNDRY_MODEL_REALTIME
            )
        self.create_response: bool = False
        self.available = self._check_configuration()
        # runtime-initialised attributes
        self._websocket: Optional[WebSocket] = None
        self.settings: Optional[AzureRealtimeExecutionSettings] = None
        self.observer = observer

    def __call__(
        self,
        create_response: bool = True
    ):
        """Prepare and return the AzureRealtimeWebsocket context manager.

        Args:
            settings (AzureRealtimeExecutionSettings): Execution settings including prompts and audio formats.
            create_response (bool): Whether to request an immediate response.
            kernel (Optional[Kernel]): Optional Semantic Kernel instance.

        Returns:
            An async context manager for AzureRealtimeWebsocket.
        """
        self.create_response = create_response
        self._raw_ws(
            settings=self.settings,
            kernel=self.kernel,
            create_response=create_response
        )

    async def _receive_events(self, audio_output_callback=None):
        """Async generator for model events, with optional audio callback."""
        async for event in self._raw_ws.receive(audio_output_callback=audio_output_callback):
            if self.observer:
                await self.observer.handle_event(event)
            yield event

    @property
    def ws(self) -> WebSocket:
        if self._websocket is None:
            raise ValueError("WebSocket not set on AOAITranslationClient")
        return self._websocket

    @ws.setter
    def ws(self, websocket: WebSocket):
        self._websocket = websocket

    def _check_configuration(self) -> bool:
        """Verify required environment variables are set and log their values for debugging."""
        required_vars = [
            ("FOUNDRY_KEY", FOUNDRY_KEY),
            ("FOUNDRY_ENDPOINT", FOUNDRY_ENDPOINT),
            ("FOUNDRY_MODEL_VERSION", FOUNDRY_MODEL_VERSION),
            ("FOUNDRY_MODEL_REALTIME", FOUNDRY_MODEL_REALTIME),
        ]
        missing = [var for var, val in required_vars if not val]
        for var, val in required_vars:
            logger.info(f"[AOAITranslationClient] {var}={'SET' if val else 'MISSING'}")
        if missing:
            logger.warning(f"[AOAITranslationClient] Azure OpenAI client missing configuration: {', '.join(missing)}")
            return False
        return True

    @abstractmethod
    def configure(self) -> None:
        """Configure client settings before invocation.

        Must set self.settings to a valid AzureRealtimeExecutionSettings.
        """

    @abstractmethod
    async def handle_realtime_messages(self):
        """Handle real-time messages from the model and forward them to ACS.

        This method should be implemented to process incoming audio data and
        send it to the Azure Communication Services (ACS) client.
        """

    @abstractmethod
    async def _from_realtime_to_acs(self):
        """Configure client settings before invocation.

        Must set self.settings to a valid AzureRealtimeExecutionSettings.
        """

    @abstractmethod
    async def _from_acs_to_realtime(self):
        """Configure client settings before invocation.

        Must set self.settings to a valid AzureRealtimeExecutionSettings.
        """


class TranslateCommand(AOAITranslationClient):
    """Concrete translation command for bidirectional language interpretation.

    Methods:
        configure(entry_language, exit_language): Set up translation prompt and audio settings.
    """

    def configure(self, entry_language: str, exit_language: str) -> None:
        """Configure the translation prompt and audio settings.

        Args:
            entry_language (str): Language code of the source audio.
            exit_language (str): Language code for the translated output.

        Raises:
            ValueError: If environment variables are missing.
        """
        instruction_template = JINJA_ENV.get_template("consignado.jinja")
        if self.available:
            self.settings = AzureRealtimeExecutionSettings(
                instructions=instruction_template.render(
                    entry_language=entry_language,
                    exit_language=exit_language
                ).strip(),
                turn_detection={"type": "server_vad"},
                voice="shimmer",
                input_audio_format="pcm16",
                output_audio_format="pcm16",
                input_audio_transcription={"model": "whisper-1"},
                function_choice_behavior=FunctionChoiceBehavior.Auto(),
            )

    async def _from_realtime_to_acs(self, audio: ndarray):
        logger.debug("Audio received from the model, sending to ACS client")
        try:
            await self.ws.send_json(
                {"Kind": "AudioData", "AudioData": {"data": base64.b64encode(audio.tobytes()).decode("utf-8")}}
            )
        except Exception as e:
            logger.error(f"[TranslateCommand] Error sending audio to ACS: {e}")

    async def handle_realtime_messages(self):
        """Forward model-generated audio to ACS via WebSocket, optimized for low latency and with detailed logging."""
        logger.info("[TranslateCommand] Starting _from_realtime_to_acs audio streaming loop.")
        try:
            async for event in self._raw_ws.receive(audio_output_callback=self._from_realtime_to_acs):
                match event.service_type:
                    case ListenEvents.SESSION_CREATED:
                        logger.info("Session Created Message")
                        logger.debug(f"  Session Id: {event.service_event.session.id}")  #type: ignore
                    case ListenEvents.ERROR:
                        logger.error(f"  Error: {event.service_event.error}")  #type: ignore
                    case ListenEvents.INPUT_AUDIO_BUFFER_CLEARED:
                        logger.info("[TranslateCommand] Input audio buffer cleared.")
                    case ListenEvents.INPUT_AUDIO_BUFFER_SPEECH_STARTED:
                        logger.debug(f"Voice activity detection started at {event.service_event.audio_start_ms} [ms]")  #type: ignore
                        await self.ws.send_json({"Kind": "StopAudio", "AudioData": None, "StopAudio": {}})
                    case ListenEvents.CONVERSATION_ITEM_INPUT_AUDIO_TRANSCRIPTION_COMPLETED:
                        logger.info(f" User:-- {event.service_event.transcript}")  #type: ignore
                    case ListenEvents.CONVERSATION_ITEM_INPUT_AUDIO_TRANSCRIPTION_FAILED:
                        logger.error(f"  Error: {event.service_event.error}")  #type: ignore
                    case ListenEvents.RESPONSE_DONE:
                        logger.info("Response Done Message")
                        logger.debug(f"  Response Id: {event.service_event.response.id}")  #type: ignore
                        if event.service_event.response.status_details:  #type: ignore
                            logger.debug(
                                f"  Status Details: {event.service_event.response.status_details.model_dump_json()}"  #type: ignore
                            )
                    case ListenEvents.RESPONSE_AUDIO_TRANSCRIPT_DONE:
                        logger.info(f" AI:-- {event.service_event.transcript}")  #type: ignore
                    case _:
                        logger.warning(f"[TranslateCommand] Received unexpected event type: {type(event)}")
        except Exception as exc:
            logger.error(f"[TranslateCommand] Exception in _from_realtime_to_acs: {exc}")
            raise
        logger.info("[TranslateCommand] Exiting _from_realtime_to_acs audio streaming loop.")

    async def _from_acs_to_realtime(self):
        """Forward audio coming in from ACS (binary frames) into the Azure OpenAI websocket."""
        while True:
            try:
                msg = await self.ws.receive_json()
                setattr(self.ws, "last_activity", time.time())
                if msg.get("text"):
                    try:
                        if msg["text"].strip().startswith("{"):
                            payload = json.loads(msg["text"])
                        else:
                            logger.warning("[TranslateCommand] Received text frame that does not look like JSON, skipping.")
                            continue
                    except Exception as e:
                        logger.error(f"[TranslateCommand] Error decoding JSON from ACS: {str(e)}")
                        continue
                else:
                    payload = msg
                    if payload.get("kind") == "AudioMetadata":
                        continue
                    if payload.get("kind") == "AudioData":
                        await self._raw_ws.send(
                            event=RealtimeAudioEvent(
                                audio=AudioContent(
                                    data=payload["audioData"]["data"],
                                    data_format="base64",
                                    inner_content=msg.get("text")
                                )
                            )
                        )
                        continue
            except Exception as e:
                logger.info(f"[TranslateCommand] WebSocket connection closed or error: {e}")
                break
            logger.info("[TranslateCommand] Exiting _from_acs_to_realtime audio streaming loop.")


@runtime_checkable
class RealtimeSession(Protocol):
    """Protocolo mínimo que o Invoker necessita.

    Define ws como property com setter para ser compatível com classes que expõem
    a conexão via @property. Implementações podem (ou não) possuir um websocket
    de modelo (_raw_ws).
    """

    @property
    def ws(self) -> WebSocket: ...  # noqa: D401

    @ws.setter
    def ws(self, websocket: WebSocket) -> None: ...

    def configure(self, *args, **kwargs) -> None: ...  # noqa: D401
    async def handle_realtime_messages(self): ...
    async def _from_realtime_to_acs(self, *args, **kwargs): ...
    async def _from_acs_to_realtime(self, *args, **kwargs): ...


class Invoker:
    """Invoker context manager for running translation commands.

    Orchestrates the execution of AOAITranslationClient commands, manages context, and provides methods for audio forwarding between ACS and Azure OpenAI.

    Methods:
        __aenter__(): Enter context, configure and open websocket.
        __aexit__(): Exit context, close websocket.
        _from_realtime_to_acs(audio): Forward audio to ACS.
        _from_acs_to_realtime(client): Forward audio to Azure OpenAI.
    """

    def __init__(
        self,
        command: RealtimeSession,
        create_response: bool = True,
        kernel: Optional[Kernel] = None
    ):
        """Initialize the invoker with a command and kernel.

        Args:
            command (AOAITranslationClient): A configured AOAITranslationClient subclass.
            kernel (Optional[Kernel]): Semantic Kernel instance for function calls.
            create_response (bool): Whether to request immediate AI response.
        """
        self.command = command
        self.kernel = kernel
        self.create_response = create_response

    async def __aenter__(self):
        """Enter context: configure command and open websocket."""
        raw_ws = getattr(self.command, "_raw_ws", None)
        if callable(getattr(self.command, "__call__", None)) and raw_ws is not None:
            # preparar contexto do websocket de modelo
            try:
                self.command(self.create_response)  # type: ignore[call-arg]
            except TypeError:
                # fallback para assinatura nomeada
                self.command(create_response=self.create_response)  # type: ignore[misc]
            await raw_ws.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context: cleanly close the websocket connection."""
        raw_ws = getattr(self.command, "_raw_ws", None)
        if raw_ws is not None:
            await raw_ws.__aexit__(exc_type, exc_val, exc_tb)

    async def start(self) -> None:
        await self.command.ws.accept()

    async def open_if_closed(self):
        """Re-open the websocket connection if it is closed. Safe to call before sending/receiving new data."""
        ws = getattr(self.command, "_raw_ws", None)
        if ws is None:
            logger.warning("[Invoker] No _raw_ws found on command; cannot check connection state.")
            return
        # Check if the websocket is closed (assume _raw_ws has an 'closed' or 'is_closed' property, fallback to context manager state)
        is_closed = getattr(ws, "closed", None)
        if is_closed is None:
            # Fallback: try to check if context manager has exited
            is_closed = getattr(ws, "_closed", False)
        if is_closed:
            logger.info("[Invoker] Websocket is closed, re-opening via __aenter__.")
            await ws.__aenter__()
        else:
            logger.debug("[Invoker] Websocket is already open; no action taken.")


class SpeechTranscriptionClient:
    """Cliente leve para consumir eventos de transcrição em tempo real do ACS.

    Não depende de Azure OpenAI. Implementa somente o protocolo RealtimeSession.
    """

    def __init__(self, observer: Any | None = None, session_id: str | None = None):
        self.observer = observer
        self._session_id = session_id or "unknown"
        self._websocket: Optional[WebSocket] = None
        self.available = True  # manter semântica usada em outras partes

    # Propriedade compatível com demais clientes
    @property
    def ws(self) -> WebSocket:
        if self._websocket is None:
            raise ValueError("WebSocket não definido em SpeechTranscriptionClient")
        return self._websocket

    @ws.setter
    def ws(self, websocket: WebSocket):
        self._websocket = websocket

    # Métodos exigidos pelo protocolo
    def configure(self, *_, **__):  # no-op
        return

    async def handle_realtime_messages(self):  # no-op (mantido por interface)
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            return

    async def _from_realtime_to_acs(self, *_, **__):  # não aplicável
        return

    async def _from_acs_to_realtime(self, *_, **__):
        """Loop principal: recebe mensagens do ACS e processa eventos de transcrição.

        Apenas eventos finais são persistidos; parciais são ignorados.
        """
        logger.info("[SpeechTranscriptionClient] Iniciando recepção de eventos de transcrição.")
        idle_timeout_sec = None
        try:
            idle_timeout_env = os.getenv("TRANSCRIPTION_IDLE_TIMEOUT_SEC")
            if idle_timeout_env:
                idle_timeout_sec = max(5, int(idle_timeout_env))
        except Exception:
            idle_timeout_sec = None

        last_activity = time.time()
        processed = 0
        finals = 0
        skipped = 0
        errors = 0

        try:
            while True:
                try:
                    msg = await self.ws.receive()
                except asyncio.TimeoutError:
                    logger.warning(f"[SpeechTranscriptionClient] Idle timeout reached ({idle_timeout_sec}s) without frames; continuing to wait.")
                    continue
                except Exception as rx:
                    errors += 1
                    logger.info(f"[SpeechTranscriptionClient] WebSocket encerrado ou erro de recepção: {rx}")
                    break

                last_activity = time.time()
                setattr(self.ws, "last_activity", last_activity)
                payload: dict | None = None

                # 1. If ACS sent a wrapper with 'text'
                txt = msg.get("text") if isinstance(msg, dict) else None
                if isinstance(txt, str):
                    raw_txt = txt.strip()
                    if raw_txt.startswith("{") and raw_txt.endswith("}"):
                        try:
                            payload = json.loads(raw_txt)
                        except Exception:
                            skipped += 1
                            continue
                        # Some ACS frames double-encode JSON under 'text'
                        if isinstance(payload, str) and payload.strip().startswith('{'):
                            try:
                                payload = json.loads(payload)
                            except Exception:
                                skipped += 1
                                continue
                    else:
                        skipped += 1
                        continue
                else:
                    if isinstance(msg, dict):
                        payload = msg
                    else:
                        skipped += 1
                        continue

                if not isinstance(payload, dict):
                    skipped += 1
                    continue

                kind = (payload.get("kind") or payload.get("Kind") or "").lower()
                if kind == "transcriptionmetadata":
                    # Log minimal metadata; could store if observer exposed (left simple here)
                    meta = payload.get("transcriptionMetadata") or {}
                    logger.info(
                        "[SpeechTranscriptionClient] Metadata subscriptionId=%s locale=%s locales=%s",
                        meta.get("subscriptionId"), meta.get("locale"), meta.get("locales")
                    )
                    processed += 1
                    continue
                if kind == "transcriptiondata":
                    try:
                        final = await self._handle_transcription_event(payload)
                        processed += 1
                        if final:
                            finals += 1
                    except Exception as h_err:
                        errors += 1
                        logger.error(f"[SpeechTranscriptionClient] Erro ao processar evento de transcrição: {h_err}")
                    continue
                # Not a recognized transcription frame
                skipped += 1
        finally:
            logger.info(
                f"[SpeechTranscriptionClient] Loop encerrado. processed={processed} finals={finals} skipped={skipped} errors={errors} idle_timeout_sec={idle_timeout_sec}"
            )

    async def _handle_transcription_event(self, payload: dict) -> bool:
        result = payload.get("result") or payload.get("Result") or {}
        transcript_text = (result.get("text") or result.get("Text") or "").strip()
        if not transcript_text:
            return False

        result_type = (result.get("type") or result.get("Type") or "").lower()
        if result_type and result_type != "final":
            return False

        participant_id = (
            result.get("participantId")
            or result.get("participant")
            or result.get("ParticipantId")
            or "unknown"
        )
        sentiment = result.get("sentiment") or result.get("Sentiment")
        doc = {
            "sessionId": self._session_id,
            "timestamp": int(time.time() * 1000),
            "participantId": participant_id,
            "transcript": transcript_text,
            "type": result_type or "final",
            "sentiment": sentiment,
            "source": "acs-realtime",
        }
        try:
            crud = CosmosCRUD('COSMOS_TRANSCRIPTS_DATABASE')
            await crud.create_item(doc)
            logger.debug("[SpeechTranscriptionClient] Transcrição final persistida (%s).", participant_id)
            return True
        except Exception as e:
            logger.error(f"[SpeechTranscriptionClient] Falha ao persistir transcrição: {e}")
            return False
