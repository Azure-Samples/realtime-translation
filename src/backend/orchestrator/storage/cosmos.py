import os
import time
import logging
from typing import List, Optional, AsyncIterator, Any

from azure.cosmos.aio import CosmosClient
from azure.cosmos import exceptions
from azure.identity.aio import DefaultAzureCredential

logger = logging.getLogger(__name__)


class CosmosCRUD:

    def __init__(self, container_env_var: str):
        self.cosmos_endpoint = os.getenv("COSMOS_ENDPOINT", "")
        self.database_name = os.getenv("COSMOS_DATABASE_NAME", "")
        self.container_name = os.getenv(container_env_var, "")
        if not self.cosmos_endpoint or not self.database_name or not self.container_name:
            raise ValueError("Missing required environment variables for CosmosDB configuration.")

    async def _get_container(self) -> AsyncIterator[Any]:  # returns one container instance via async generator
        """Internal helper yielding a container client ensuring DB existence.

        Uses a new CosmosClient per call to keep the surface change minimal. If performance
        becomes an issue, promote the client to a long-lived attribute with explicit close.
        """
        credential = DefaultAzureCredential()
        client = CosmosClient(self.cosmos_endpoint, credential=credential)
        async with client:  # ensures proper cleanup each call
            try:
                database = client.get_database_client(self.database_name)
                await database.read()
            except exceptions.CosmosResourceNotFoundError:
                logger.info("Creating Cosmos database '%s'", self.database_name)
                await client.create_database(self.database_name)
                database = client.get_database_client(self.database_name)
            container = database.get_container_client(self.container_name)
            yield container

    async def list_items(self, query: str = "SELECT * FROM c", parameters: Optional[List] = None):
        if parameters is None:
            parameters = []
        async for container in self._get_container():
            items = [item async for item in container.query_items(query=query, parameters=parameters)]
        return items

    async def create_item(self, item: dict):
        # Ensure mandatory 'id' field exists (Cosmos requirement)
        if 'id' not in item:
            item['id'] = f"{item.get('sessionId', 'doc')}-{item.get('timestamp', int(time.time()*1000))}"
        async for container in self._get_container():
            response = await container.upsert_item(item)
        return response

    async def read_item(self, item_id: str, partition_key: Optional[str] = None):
        pk = partition_key or item_id
        async for container in self._get_container():
            response = await container.read_item(item=item_id, partition_key=pk)
        return response

    async def update_item(self, item_id: str, item: dict, partition_key: Optional[str] = None):
        pk = partition_key or item_id
        async for container in self._get_container():
            response = await container.replace_item(item=item_id, body=item, partition_key=pk)
        return response

    async def delete_item(self, item_id: str, partition_key: Optional[str] = None):
        pk = partition_key or item_id
        async for container in self._get_container():
            response = await container.delete_item(item=item_id, partition_key=pk)
        return response
