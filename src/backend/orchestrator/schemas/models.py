"""
This module defines the data models for the application using Pydantic.

These models represent the core entities of the augmented RAG system including agents,
assemblies, tools, and various data types (text, image, audio, video) used for
retrieval-augmented generation tasks.

Classes:
    Agent: Represents an AI agent with specific capabilities and configuration.
    Assembly: Represents a collection of agents working together for a specific objective.
    Tool: Represents a utility function available to agents.
    TextData: Represents textual data with metadata and embeddings.
    ImageData: Represents image data with metadata and embeddings.
    AudioData: Represents audio data with metadata and embeddings.
    VideoData: Represents video data with metadata and embeddings.
    JobResponse: Represents the response from a processing job.
"""

from typing import Callable, List, Literal, Optional, Any
from pydantic import BaseModel, Field, field_validator


class Agent(BaseModel):
    """
    Represents an AI agent with its configuration and capabilities.

    Attributes:
        id (str): The unique identifier for the agent.
        name (str): The human-readable name of the agent.
        model_id (str): The identifier of the model used by this agent.
        metaprompt (str): The system prompt that defines the agent's behavior.
        objective (Literal["image", "text", "audio", "video"]): The data type this agent specializes in.
    """

    id: int = Field(..., description="Agent ID")
    name: str = Field(..., description="Agent Name")
    model_id: str = Field(..., description="Model ID")
    metaprompt: str = Field(..., description="Agent System Prompt")
    objective: Literal["image", "text", "audio", "video"] = Field(default='text', description="Agent Objective")

    @classmethod
    @field_validator("model_id")
    def model_must_be_small(cls, v):
        if len(v) > 32:
            raise ValueError("model ID shouldn't have more than 32 characters")
        return v

    @classmethod
    @field_validator("objective")
    def objective_must_be_small(cls, v):
        if len(v) > 32:
            raise ValueError("objective shouldn't have more than 32 characters")
        return v


class Swarm(BaseModel):
    """
    Represents a collection of agents working together toward a specific objective.

    Attributes:
        id (str): The unique identifier for the assembly.
        objective (str): The goal or task this assembly is designed to achieve.
        agents (List[Agent]): The collection of agents that form this assembly.
        roles (List[str]): The defined roles for agents within this assembly.
    """

    id: int = Field(..., description="Agent Swarm ID")
    objective: str = Field(..., description="The Agent Swarm Object to operate on")
    agents: List[Agent] = Field(..., description="Agents Swarms")
    roles: List[str] = Field(..., description="Agent Roles ID")
    order: Optional[List[int]] = Field(default=None, description="Agent Order of Execution")

    @classmethod
    @field_validator("roles")
    def roles_must_not_exceed_length(cls, v):
        for role in v:
            if len(role) > 360:
                raise ValueError("each role must have at most 360 characters")
        return v

    @classmethod
    @field_validator("order")
    def orders_must_contain_ids(cls, v):
        for order in v:
            if order not in [agent.id for agent in v.agents]:
                raise ValueError("each role must have at most 360 characters")
        return v


class Tool(BaseModel):
    """
    Represents a tool that can be used by agents to perform specific functions.

    Attributes:
        id (str): The unique identifier for the tool.
        name (str): The name of the tool.
        description (str): A description of what the tool does.
        func (Callable[..., Any]): The executable function that implements the tool's functionality.
    """
    id: str = Field(..., description="Tool ID")
    name: str = Field(..., description="Tool Name")
    description: str = Field(..., description="Tool Description")
    func: Callable[..., Any] = Field(..., description="Tool Function")


class CallConnectionInfo(BaseModel):
    caller_id: str
    room_id: str
    callback_uri: str
    websocket_url: str
    call_connection_id: str
    bot_display_name: str
    bot_language: str
    bot_id: Optional[str] = None
    media_streaming_subscription: Optional[str] = None
    last_event_data: Optional[dict] = None
    transcription_failed: bool = False
    transcription_error_code: Optional[int] = None
    transcription_error_subcode: Optional[int] = None
    transcription_error_message: Optional[str] = None
    transcription_status_details: Optional[str] = None
    transcription_last_failure_time: Optional[int] = None  # epoch ms
    transcription_metadata: Optional[dict] = None
    # Deferred transcription start lifecycle tracking
    transcription_start_requested: bool = False
    transcription_started: bool = False
    transcription_start_attempts: int = 0
    transcription_last_start_error: Optional[str] = None
    transcription_last_start_time: Optional[int] = None  # epoch ms of last attempt