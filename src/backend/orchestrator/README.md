# Orchestrator Backend Overview

This backend orchestrates real-time audio and text translation using Azure OpenAI, Azure Communication Services (ACS), and Event Grid. It manages user sessions, WebSocket audio streaming, ACS call automation events, and coordinates translation using the Command and Observer patterns.

## Architecture Overview

- **FastAPI** is used as the main web framework, providing REST and WebSocket endpoints.
- **Azure Communication Services (ACS)** handles call automation, media streaming, and participant management.
- **Azure OpenAI** is used for real-time translation of audio streams.
- **Event Grid** is used for event-driven communication, especially for ACS call events.
- **Command and Observer Patterns** are used for orchestration, extensibility, and decoupling of translation and call logic.

## Application Lifecycle and Lifespan Logic

The application uses FastAPI's `lifespan` context to manage global resources and background tasks:

- **room_user_observer**: A singleton observer instance is created for the entire app. It manages all rooms, users, ACS calls, and translation invokers.
- **Background Tasks**:
  - `resource_cleanup_task`: Runs every 60 seconds. It checks all active ACS sessions (using `room_user_observer._connection_session` as the source of truth). If a session is inactive (no audio/video for 5+ minutes), it unregisters the websocket, removes the session, and hangs up the ACS call to avoid extra spend.
  - `translation_orchestration_task`: Runs every second. It supervises all active translation invoker tasks, restarting any that are missing or have crashed.
- On shutdown, both background tasks are cancelled and awaited for graceful cleanup.

## WebSocket Endpoint and Bot Awareness

The `/ws` endpoint handles real-time audio streaming and translation. For full integration with ACS calls and the Interpreter bot:

- The websocket is registered with the observer and associated with the correct ACS call and session using `room_user_observer.register_websocket(session_id, websocket)` and `room_user_observer.map_connection_to_session(call_connection_id, session_id)`.
- The translation invoker is registered for each call connection, enabling routing of audio and translation events to the correct websocket and bot.
- The translation loop continues until the WebSocket is disconnected, ensuring low-latency, real-time translation.

## Incoming Call Handling and Bot Orchestration

- The `/incoming-call` endpoint receives Event Grid events for new ACS calls.
  - It answers the call, registers the call in the observer, and pre-registers an Interpreter bot for the room/call.
  - The call connection is mapped to the session for later lookup.
- The `/callbacks/{contextId}` endpoint receives ACS callback events (e.g., CallConnected, MediaStreamingStarted).
  - On `CallConnected`, it adds the bot as a participant and, if a websocket is registered, starts a translation invoker for the call.

## Session Cleanup Logic

- The cleanup task uses `room_user_observer._connection_session` as the authoritative list of active ACS sessions.
- For each session, it checks the last activity timestamp on the associated websocket.
- If inactive for 5+ minutes, it unregisters the websocket, removes the session, and hangs up the ACS call.
- This ensures orphaned or idle sessions are cleaned up and resources are released both in the app and in ACS.

## Key Components

| Component                        | Description                                                                                   | Scope                |
|-----------------------------------|-----------------------------------------------------------------------------------------------|----------------------|
| `lifespan`                       | Initializes observer, starts cleanup and translation tasks, manages app lifecycle              | Whole app            |
| `resource_cleanup_task`           | Cleans up inactive ACS sessions, unregisters websockets, hangs up calls                       | All sessions/calls   |
| `translation_orchestration_task`  | Supervises translation invokers, restarts if needed                                           | All calls            |
| `/ws` endpoint                    | Handles websocket connections, registers websocket and invoker, links to bot/call/session      | Per websocket/client |
| `/incoming-call` handler          | Answers ACS calls, registers bot, maps call to session, notifies observer                     | Per ACS call         |
| `/callbacks/{contextId}`          | Handles ACS call events, adds bot, starts translation when possible                           | Per ACS call         |

## Recommendations

- Start the devtunnel with `devtunnel host`;
- Ensure the `/ws` endpoint registers websockets and invokers with the observer and associates them with the correct call/session for full orchestration.
- Use the observer and background tasks to ensure robust, scalable, and cost-effective management of all translation and call resources.
- Follow Azure best practices for security, scalability, and maintainability.

## Azure Integration Notes

- All ACS and translation operations are performed using Azure SDKs and recommended patterns.
- Event-driven architecture ensures the backend can scale and respond to real-time events efficiently.
- Resource cleanup and session management are designed to minimize Azure resource usage and cost.

---
