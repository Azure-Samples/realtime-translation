"""
Observer pattern and ACS event handling for real-time translation backend.

This module defines the Subject/Observer pattern for managing user/room/call state, and integrates
Azure Communication Services (ACS) event handling for call automation and media streaming.
It also provides session and WebSocket mapping for real-time translation.
"""

from abc import ABC, abstractmethod

import os
import uuid
import asyncio
from asyncio import sleep as _async_sleep

from urllib.parse import urlencode
from typing import Any, Optional

from azure.communication.callautomation import (
    CallAutomationClient,
    MediaStreamingOptions,
    TranscriptionOptions,
    TranscriptionTransportType
)
from azure.communication.rooms import RoomsClient, RoomParticipant, ParticipantRole
from azure.communication.identity import CommunicationIdentityClient
from azure.eventgrid import EventGridEvent, SystemEventNames

from fastapi import WebSocket
from orchestrator.engine import Invoker, TranslateCommand
from orchestrator.schemas.models import CallConnectionInfo
from orchestrator import logger


ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")
CALLBACK_URI = os.environ.get("CALLBACK_EVENTS_URI", "https://v6hw08k2-9000.brs.devtunnels.ms/callbacks")
WEBSOCKET_URI = os.environ.get('WEBSOCKET_EVENTS_URI', 'wss://v6hw08k2-9000.brs.devtunnels.ms/transcript-ws')
FOUNDRY_SPEECH_ENDPOINT = os.environ.get('FOUNDRY_SPEECH_ENDPOINT', '')


class Subject(ABC):
    """Abstract base class for subjects in the Observer pattern.

    Methods:
        attach(observer): Attach an observer.
        detach(observer): Detach an observer.
        notify(event): Notify all observers of an event.
    """

    @abstractmethod
    def attach(self, observer: 'Observer') -> None:
        """Attach an observer to the subject."""

    @abstractmethod
    def detach(self, observer: 'Observer') -> None:
        """Detach an observer from the subject."""

    @abstractmethod
    def notify(self, event: 'RoomUserEvent') -> None:
        """Notify all observers about an event."""


class Observer(ABC):
    """Abstract base class for observers in the Observer pattern.

    Methods:
        update(subject, event): Receive update from subject.
    """

    @abstractmethod
    def update(self, subject: Subject, event: 'RoomUserEvent') -> None:
        """Receive update from subject."""


class RoomUserEvent:
    """Represents an event in the room/user system (join, leave, call, etc).

    Attributes:
        event_type (str): The type of event.
        user_id (str): The user ID involved.
        room_id (str): The room ID involved.
        data (dict): Additional event data.
    """

    def __init__(self, event_type: str, user_id: Optional[str] = None, room_id: Optional[str] = None, data: Optional[dict] = None):
        self.event_type = event_type
        self.user_id = user_id
        self.room_id = room_id
        self.data = data or {}


class RoomUserObserver(Subject):
    """Observer subject for managing room/user state and ACS event handling.

    Manages rooms, users, sessions, and call connection mappings.
    Handles ACS incoming call and callback events, and triggers translation using the Command pattern.

    Methods:
        handle_incoming_call(event_dict): Handle ACS incoming call events.
        handle_callback_event(event): Handle ACS callback events.
        register_websocket(session_id, websocket): Register a WebSocket for a session.
        get_websocket_for_connection(call_connection_id): Get WebSocket for a call connection.
    """

    def __init__(self):
        self._observers: set[Observer] = set()
        self._rooms: dict[str, set[str]] = {}
        self._user_room: dict[str, str] = {}
        self._user_info: dict[str, dict[str, Any]] = {}
        self._call_connections: dict[str, CallConnectionInfo] = {}
        self._invokers: dict[str, Invoker] = {}
        acs_conn_str = os.environ.get("ACS_CONNECTION_STRING", "")
        self.acs_client = CallAutomationClient.from_connection_string(acs_conn_str)
        self.identity_client = CommunicationIdentityClient.from_connection_string(acs_conn_str)
        self.rooms_client = RoomsClient.from_connection_string(acs_conn_str)

    def attach(self, observer: 'Observer') -> None:
        """Attach an observer to the subject."""
        self._observers.add(observer)

    def detach(self, observer: 'Observer') -> None:
        """Detach an observer from the subject."""
        self._observers.discard(observer)

    def notify(self, event_type: str, payload: dict) -> None:
        """Notify all observers with a RoomUserEvent."""
        event = RoomUserEvent(
            event_type=event_type,
            user_id=payload.get('user_id'),
            room_id=payload.get('room_id'),
            data=payload
        )
        for observer in self._observers:
            observer.update(self, event)

    def register_invoker(self, call_connection_id: str, invoker: Invoker):
        """Register an Invoker for a call connection. Only one invoker per connection is allowed."""
        if call_connection_id in self._invokers:
            logger.warning(f"Invoker already exists for call_connection_id={call_connection_id}, skipping registration.")
            return
        logger.info(f"Registering invoker for call_connection_id={call_connection_id}")
        self._invokers[call_connection_id] = invoker

    def enable_invoker(self, call_connection_id: str, websocket: WebSocket):
        """Enable an Invoker by attaching a WebSocket for a call connection. Create and register if not exists."""
        logger.info(f"Enabling invoker for call_connection_id={call_connection_id}")
        invoker = self._invokers.get(call_connection_id)
        if not invoker:
            # Create and register the invoker only when websocket is active
            translate_command = TranslateCommand()
            # You may want to set entry/exit language dynamically if needed
            translate_command.configure(entry_language="en", exit_language="zh")
            invoker = Invoker(command=translate_command)
            self.register_invoker(call_connection_id, invoker)
            logger.info(f"Invoker created and registered for call_connection_id={call_connection_id}")
        # Only attach if not already attached or if websocket is different
        if getattr(invoker.command, 'ws', None) is not None and invoker.command.ws is websocket:
            logger.info(f"WebSocket already attached to invoker for call_connection_id={call_connection_id}")
            logger.info(f"[INVOKER STATUS] Invoker already attached for call_connection_id={call_connection_id}")
            return invoker
        invoker.command.ws = websocket
        logger.info(f"WebSocket attached to invoker for call_connection_id={call_connection_id}")
        logger.info(f"[INVOKER STATUS] Invoker attached for call_connection_id={call_connection_id}")
        # Ensure websocket is open before returning invoker
        # (This is the new logic: try to re-open if closed)
        # This is safe to call even if already open
        try:
            # If not in an event loop, schedule open_if_closed
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(invoker.open_if_closed())
            else:
                loop.run_until_complete(invoker.open_if_closed())
        except Exception as e:
            logger.warning(f"Could not ensure websocket is open for invoker: {e}")
        return invoker

    async def cleanup_invoker(self, call_connection_id: str):
        """Properly close and remove the invoker for a call connection on disconnect."""
        invoker = self._invokers.get(call_connection_id)
        if invoker:
            try:
                await invoker.__aexit__(None, None, None)
                logger.info(f"Closed invoker for call_connection_id={call_connection_id}")
                logger.info(f"[INVOKER STATUS] Invoker closed for call_connection_id={call_connection_id}")
            except Exception as e:
                logger.error(f"Error closing invoker for call_connection_id={call_connection_id}: {e}")
        self.unregister_invoker(call_connection_id)

    def unregister_invoker(self, call_connection_id: str):
        """Remove an Invoker mapping for a call connection."""
        if call_connection_id in self._invokers:
            logger.info(f"Unregistering invoker for call_connection_id={call_connection_id}")
            logger.info(f"[INVOKER STATUS] Invoker unregistered for call_connection_id={call_connection_id}")
        else:
            logger.warning(f"Tried to unregister invoker for non-existent call_connection_id={call_connection_id}")
        self._invokers.pop(call_connection_id, None)

    def get_invoker_for_connection(self, call_connection_id: str):
        """Retrieve the Invoker for a given call connection ID."""
        return self._invokers.get(call_connection_id)

    async def handle_incoming_call(self, event_dict, enable_transcription: bool = True, enable_media: bool = False):
        """Handles EventGrid events for ACS incoming calls using the ACS SDK."""
        event = EventGridEvent.from_dict(event_dict)
        match event.event_type:
            case SystemEventNames.EventGridSubscriptionValidationEventName:
                return self._handle_subscription_validation(event)
            case 'Microsoft.Communication.CallStarted':
                return await self._handle_acs_incoming_call(event, enable_transcription=enable_transcription, enable_media=enable_media)
            case _:
                self.notify('incoming_call_unhandled', {"event": event_dict})
                return None

    def _handle_subscription_validation(self, event):
        """Handle EventGrid subscription validation event."""
        validation_code = event.data["validationCode"]
        return {"validationResponse": validation_code}

    async def _handle_acs_incoming_call(self, event, enable_transcription: bool = True, enable_media: bool = False):
        """Handle ACS incoming call event with selectable modes."""
        caller_id = event.data["startedBy"]["communicationIdentifier"]["rawId"]
        room_id = event.data["room"]["id"]
        guid = uuid.uuid4()
        callback_uri, websocket_url = self._build_callback_uris(caller_id, guid)
        if not self.acs_client:
            raise RuntimeError("ACS client not initialized. Set AZURE_COMMUNICATION_CONNECTION_STRING.")

        # Conflict resolution: prefer transcription if both requested
        chosen_mode = None
        if enable_transcription and enable_media:
            logger.warning("Both transcription and media streaming requested; preferring transcription to avoid known conflicts (8523).")
            enable_media = False
        try:
            if enable_transcription:
                answer_call_result = self._connect_call_transcription(room_id, callback_uri, websocket_url)
                chosen_mode = "transcription"
            elif enable_media:
                answer_call_result = self._connect_call_media_streaming(room_id, callback_uri, websocket_url)
                chosen_mode = "media"
            else:
                # Fallback: at least establish call without extra streaming features
                logger.info("No transcription or media streaming requested. Connecting call with minimal configuration.")
                answer_call_result = self.acs_client.connect_call(
                    room_id=room_id,
                    operation_context="incomingCall",
                    callback_url=callback_uri
                )
                chosen_mode = "basic"
        except Exception as e:
            logger.error(f"Failed to answer ACS call ({caller_id} -> room {room_id}): {e}")
            raise RuntimeError(f"Failed to answer ACS call: {e}")

        call_connection_id = answer_call_result.call_connection_id
        if not call_connection_id:
            logger.error("Failed to answer ACS call, no call_connection_id returned.")
            raise RuntimeError("Failed to answer ACS call, no call_connection_id returned.")

        bot_info = {"display_name": "Interpreter", "language": "en"}
        self._call_connections[call_connection_id] = CallConnectionInfo(**{
            "caller_id": caller_id,
            "room_id": room_id,
            "callback_uri": callback_uri,
            "websocket_url": websocket_url,
            "call_connection_id": answer_call_result.call_connection_id,
            "bot_display_name": bot_info["display_name"],
            "bot_language": bot_info["language"],
            # Mark that we intend to start transcription if transcription mode was enabled
            "transcription_start_requested": enable_transcription,
            "transcription_started": False
        })
        logger.info(f"Incoming call answered mode={chosen_mode} call_connection_id={call_connection_id} transcription={enable_transcription} media={enable_media}")
        self.notify('incoming_call', self._call_connections[call_connection_id].model_dump())
        return self._call_connections[call_connection_id]

    def _connect_call_transcription(self, room_id: str, callback_uri: str, websocket_url: str):
        transcription_options = TranscriptionOptions(
            transport_url=websocket_url,
            transport_type=TranscriptionTransportType.WEBSOCKET,
            locale="en-US",
            start_transcription=False  # we will explicitly invoke start after connect
        )
        connect_result = self.acs_client.connect_call(
            room_id=room_id,
            operation_context="incomingCall",
            callback_url=callback_uri,
            transcription=transcription_options,
            cognitive_services_endpoint=FOUNDRY_SPEECH_ENDPOINT or None
        )
        # Defer explicit start until CallConnected event to avoid 8501 (call not Established)
        return connect_result

    def _connect_call_media_streaming(self, room_id: str, callback_uri: str, websocket_url: str):
        media_streaming_options = MediaStreamingOptions(
            transport_url=websocket_url,
            transport_type="websocket",
            content_type="audio",
            audio_channel_type="unmixed",
            start_media_streaming=True,
            enable_bidirectional=False,
            audio_format="pcm16KMono"
        )  # type: ignore
        return self.acs_client.connect_call(
            room_id=room_id,
            operation_context="incomingCall",
            callback_url=callback_uri,
            media_streaming=media_streaming_options
        )

    def _build_callback_uris(self, caller_id, guid):
        """Build callback and websocket URIs for ACS call answer."""
        query_parameters = urlencode({"callerId": caller_id})
        callback_uri = f"{CALLBACK_URI}/{guid}?{query_parameters}"
        # Provide a per-call unique websocket URL (query token) so simultaneous calls don't collide
        websocket_url = f"{WEBSOCKET_URI}?cid={guid}"
        return callback_uri, websocket_url

    async def handle_callback_event(self, event):
        """Handles callbacks from ACS (CallConnected, MediaStreamingStarted, etc). Triggers translation command on CallConnected."""
        event_data = event["data"]
        call_connection_id = event_data.get("callConnectionId")
        event_type = event["type"]

        match event_type:
            case "Microsoft.Communication.CallConnected":
                await self._handle_call_connected(event_data, call_connection_id)
            case "Microsoft.Communication.CallDisconnected":
                await self._handle_call_disconnected(event_data, call_connection_id)
            case "Microsoft.Communication.TranscriptionFailed":
                await self._handle_transcription_failed(event_data, call_connection_id)
            case "Microsoft.Communication.MediaStreamingStarted":
                await self._handle_media_streaming_started(event_data, call_connection_id)
            case "Microsoft.Communication.MediaStreamingStopped":
                await self._handle_media_streaming_stopped(event_data, call_connection_id)
            case "Microsoft.Communication.MediaStreamingFailed":
                await self._handle_media_streaming_failed(event_data, call_connection_id)
            case "Microsoft.Communication.ParticipantsUpdated":
                await self._handle_participant_updated(event_data, call_connection_id)
            case "Microsoft.Communication.AddParticipantFailed":
                await self._handle_add_participant_failed(event_data, call_connection_id)                
            case _:
                self._handle_callback_event_default(event_type, event_data, call_connection_id)
        self.notify('callback_event', {"type": event_type, "call_connection_id": call_connection_id, "data": event_data})
        return {"call_connection_id": call_connection_id, "type": event_type}

    async def _handle_call_connected(self, event_data, call_connection_id):
        logger.info(f"Connecting to call with id {call_connection_id}")
        if self.acs_client and call_connection_id:
            try:
                call_connection = self.acs_client.get_call_connection(call_connection_id)
                call_properties = call_connection.get_call_properties()
                media_streaming_subscription = getattr(call_properties, "media_streaming_subscription", None)

                self._call_connections[call_connection_id].last_event_data = event_data
                self._call_connections[call_connection_id].media_streaming_subscription = media_streaming_subscription
                call_connection.start_transcription()

                call_info = self._call_connections.get(call_connection_id, None)

                if not call_info:
                    logger.error(f"No bot_info found for call_connection_id={call_connection_id}. Cannot join bot to call.")
                    call_connection.hang_up(is_for_everyone=True)

                bot_info = {'display_name': call_info.bot_display_name, 'language': call_info.bot_language}  #type: ignore
                logger.info(f"bot_info for call_connection_id={call_connection_id}: {bot_info}")
                self.join_bot_to_acs_call(call_connection_id, bot_info)
                # Attempt deferred transcription start if it was requested and not yet started
                if call_info and getattr(call_info, 'transcription_start_requested', False) and not getattr(call_info, 'transcription_started', False):
                    asyncio.create_task(self._attempt_transcription_start(call_connection_id))
            except Exception as e:
                logger.error(f"Failed to get call properties for {call_connection_id}: {e}")

    async def _handle_media_streaming_started(self, event_data, call_connection_id):
        logger.info(f"Media streaming started for call_connection_id={call_connection_id}")
        logger.info(f"Event data:\n {event_data} ")

    async def _handle_media_streaming_stopped(self, event_data, call_connection_id):
        logger.info(f"Media streaming stopped for call_connection_id={call_connection_id}")
        logger.info(f"Event data:\n {event_data} ")
        if not call_connection_id in self._call_connections:
            logger.warning(f"Call connection {call_connection_id} not found in active connections.")

    async def _handle_media_streaming_failed(self, event_data, call_connection_id):
        logger.error(f"Media streaming failed for call_connection_id={call_connection_id}")
        logger.info(f"Event data:\n {event_data} ")

    async def _handle_transcription_failed(self, event_data, call_connection_id):
        logger.warning(f"Transcription failed for call_connection_id={call_connection_id}")
        info = self._call_connections.get(call_connection_id)
        if info:
            result_info = event_data.get('resultInformation', {})
            status_details = (event_data.get('transcriptionUpdate') or {}).get('transcriptionStatusDetails')
            import time as _t
            info.transcription_failed = True
            info.transcription_error_code = result_info.get('code')
            info.transcription_error_subcode = result_info.get('subCode')
            info.transcription_error_message = result_info.get('message')
            info.transcription_status_details = status_details
            info.transcription_last_failure_time = int(_t.time() * 1000)
            self._call_connections[call_connection_id] = info
        logger.error(
            f"[TranscriptionFailed] call_connection_id={call_connection_id} code={event_data.get('resultInformation', {}).get('code')} "
            f"subCode={event_data.get('resultInformation', {}).get('subCode')} "
            f"statusDetails={(event_data.get('transcriptionUpdate') or {}).get('transcriptionStatusDetails')}"
        )

    async def _handle_call_disconnected(self, event_data, call_connection_id):
        logger.warning(f"Call disconnected for call_connection_id={call_connection_id}")
        # attempt cleanup of any invoker (translation) resources
        try:
            await self.cleanup_invoker(call_connection_id)
        except Exception as e:
            logger.error(f"Error during invoker cleanup on disconnect ({call_connection_id}): {e}")
        # remove call connection info
        if call_connection_id in self._call_connections:
            self._call_connections.pop(call_connection_id, None)
        logger.info(f"Resources released for call_connection_id={call_connection_id}")

    async def _handle_participant_updated(self, event_data, call_connection_id):
        call_data = self._call_connections.get(call_connection_id)
        participants = [
            participant.communication_identifier.raw_id
            for participant in self.rooms_client.list_participants(room_id=call_data.room_id)  # type: ignore
        ]
        logger.info(f"Participants updated for call_connection_id={call_connection_id}: {participants}")

    async def _handle_add_participant_failed(self, event_data, call_connection_id):
        logger.error(f"Failed to add participant to call_connection_id={call_connection_id}: {event_data.get('errorMessage', 'Unknown error')}")
        call_connection = self.acs_client.get_call_connection(call_connection_id)
        call_connection.hang_up(is_for_everyone=True)
        logger.error(f"Failed to add participant to call_connection_id={call_connection_id}. Hanging up call.")

    def _handle_callback_event_default(self, event_type, event_data, call_connection_id):
        logger.warning(f"Unhandled ACS callback event: {event_type} for call_connection_id={call_connection_id}")
        # Some events may signal readiness; optionally attempt transcription start if still pending
        if call_connection_id and event_type.endswith("ParticipantsUpdated"):
            info = self._call_connections.get(call_connection_id)
            if info and info.transcription_start_requested and not info.transcription_started:
                asyncio.create_task(self._attempt_transcription_start(call_connection_id))

    async def _attempt_transcription_start(self, call_connection_id: str, max_retries: int = 5, backoff: float = 1.0):
        """Attempt to start transcription with retry/backoff if call not yet Established.

        Handles 8501 (Action invalid when call is not in Established state) by retrying
        with exponential backoff up to max_retries. Marks state fields in CallConnectionInfo.
        """
        info = self._call_connections.get(call_connection_id)
        if not info:
            logger.error(f"Cannot attempt transcription start; no CallConnectionInfo for {call_connection_id}")
            return
        info.transcription_start_requested = True
        self._call_connections[call_connection_id] = info
        for attempt in range(1, max_retries + 1):
            try:
                import time as _t
                info.transcription_start_attempts = attempt
                info.transcription_last_start_time = int(_t.time() * 1000)
                self._call_connections[call_connection_id] = info
                call_connection = self.acs_client.get_call_connection(call_connection_id)
                call_connection.start_transcription()
                info.transcription_started = True
                info.transcription_last_start_error = None
                self._call_connections[call_connection_id] = info
                logger.info(f"Transcription started successfully after attempt {attempt} for call_connection_id={call_connection_id}")
                return
            except Exception as e:  # Broad catch; Azure SDK raises HttpResponseError
                msg = str(e)
                info.transcription_last_start_error = msg
                self._call_connections[call_connection_id] = info
                if "8501" in msg or "not in Established" in msg:
                    if attempt < max_retries:
                        sleep_time = backoff * (2 ** (attempt - 1))
                        logger.warning(f"Call not yet established (8501) for call_connection_id={call_connection_id}; retrying in {sleep_time:.2f}s (attempt {attempt}/{max_retries})")
                        await _async_sleep(sleep_time)
                        continue
                logger.error(f"Failed to start transcription for call_connection_id={call_connection_id} (attempt {attempt}/{max_retries}): {e}")
                if attempt >= max_retries:
                    logger.error(f"Exhausted transcription start retries for call_connection_id={call_connection_id}")
                # Non-retryable or max attempts reached; exit loop
                break

    def join_bot_to_acs_call(self, connection_id: str, bot_info: dict):
        """
        Prepare the bot to join the ACS call for the given room.
        The bot will be added as a participant after the call is established (in _handle_call_connected).
        """
        logger.info(
            "Adding bot '%s' as participant to ACS call (call_connection_id=%s)",
            bot_info.get('bot_display_name', 'Bot'),
            connection_id
        )
        bot_user, bot_token = self.identity_client.create_user_and_token(scopes=["voip", "chat"])
        current_connection = self._call_connections[connection_id]
        current_connection.bot_id = bot_user.raw_id

        room = self.rooms_client.get_room(current_connection.room_id)
        if not room:
            logger.error(f"Room with ID {current_connection.room_id} not found.")
            raise RuntimeError(f"Room with ID {current_connection.room_id} not found.")

        participant = RoomParticipant(
            communication_identifier=bot_user,  # type: ignore
            role=ParticipantRole.PRESENTER  # Adjust role as needed
        )

        try:
            self.rooms_client.add_or_update_participants(
                room_id=current_connection.room_id,
                participants=[participant]
            )
            logger.info(f"Bot successfully added as participant to room {current_connection.room_id} with ID {bot_user.raw_id}.")
        except Exception as e:
            logger.error(f"Failed to add bot as participant: {e}")

        logger.info(f"Bot info stored for room {connection_id}, will add after call is established.")


class RoomUserEventObserver(Observer):
    """Example observer that reacts to room/user events."""

    def update(self, subject: RoomUserObserver, event: RoomUserEvent) -> None:
        """React to room/user events."""
        if event.event_type == 'user_joined':
            pass
        elif event.event_type == 'user_left':
            pass
        elif event.event_type.startswith('call'):
            pass


class LoggingObserver(Observer):
    """Observer that logs all events for debugging and monitoring."""

    def update(self, subject: Subject, event: RoomUserEvent) -> None:
        """Log the event details."""
        print(f"[LoggingObserver] Event: {event.event_type}, \n User: {event.user_id}, \n Room: {event.room_id}, \n Data: {event.data}")
