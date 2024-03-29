import asyncio
import json
from typing import Iterable, List, Set, Tuple

from nio import AsyncClient, RoomMessagesError, RoomMessageText

from encrypted_search.index import EncryptedIndex
from encrypted_search.search import EncryptedSearch
from encrypted_search.storage import IndexStorage
from encrypted_search.types import Event, LookupTable

HOMESERVER = "https://matrix.org"
USER = "@burgers:matrix.org"
PASSWORD = "this-is-my-real-password"  # Please don't misuse my extremely real password

SOURCE_ROOM = "!vQWVXkEbAIxqvhOGmF:matrix.org"  # Room to be indexed
DESTINATION_ROOM = "!zzqwCHxHhBvTXwkRZm:matrix.org"  # Room to store index in
N1 = 20  # Number of messages in first batch
N2 = 20  # Number of messages in second batch
DOCUMENT_SIZE = 5000  # Maximum size of each document in database

SEARCH_QUERIES = [
    "matrix",
    "encrypted",
    "matrix media",
]


async def login() -> Tuple[AsyncClient, str]:
    """Logs in a Nio client with provided `USER` and `PASSWORD` and syncs up with current state.

    Returns:
        A tuple of the form (C, T) where — C is the created client and T is a token that can be used to fetch messages.
    """

    client = AsyncClient(HOMESERVER, USER)
    await client.login(PASSWORD)
    sync_response = await client.sync(timeout=10000, full_state=True)
    return client, sync_response.next_batch


async def fetch_n1_plus_n2_messages(
    client: AsyncClient,
    token: str,
) -> Tuple[List[Event], List[Event]]:
    """Fetches latest `N1` + `N2` "m.room.message" events from provided `SOURCE_ROOM`.

    Args:
        client: Authenticated Nio client
        token: Post-sync token for client

    Returns:
        A tuple of the form (A, B), where — A is a list of `N1` "m.room.message" events, and B is a similar list of length `N2`.
    """

    batch_1 = []

    # Fetch first batch
    while len(batch_1) < N1:
        # Fetch events, filtering out non-message events.
        response = await client.room_messages(
            SOURCE_ROOM,
            start=token,
            limit=N1,
            message_filter={
                "types": [
                    "m.room.message",
                ],
            },
        )

        # Quit on error or on reaching beginning
        if isinstance(response, RoomMessagesError):
            print(response)
            break
        if response.end is None:
            break

        # Only keep bare-bones of each message
        batch_1.extend([
            message.source for message in response.chunk
            if isinstance(message, RoomMessageText)
        ])

        # Update token
        token = response.end

    # Move extras to second batch
    batch_2 = batch_1[N1:]
    del batch_1[N1:]

    # Fetch second batch
    while len(batch_2) < N2:
        # Fetch events, filtering out non-message events.
        response = await client.room_messages(
            SOURCE_ROOM,
            start=token,
            limit=N2,
            message_filter={
                "types": [
                    "m.room.message",
                ],
            },
        )

        # Quit on error or on reaching beginning
        if isinstance(response, RoomMessagesError):
            print(response)
            break
        if response.end is None:
            break

        # Only keep bare-bones of each message
        batch_2.extend([
            message.source for message in response.chunk
            if isinstance(message, RoomMessageText)
        ])

        # Update token
        token = response.end

    return batch_1, batch_2


async def create_and_upload_index(
    client: AsyncClient,
    messages: List[Event],
) -> LookupTable:
    """Creates a structurally-encrypted index and uploads it to the provided `DESTINATION_ROOM` as messages.

    Args:
        client: Authenticated Nio client
        messages: List of "m.room.message" events

    Returns:
        A lookup table from a keyword to corresponding location in the `DESTINATION_ROOM`
    """

    # Create the basic structurally-encrypted index
    encrypted_index = EncryptedIndex(messages)

    # Prepare the index for upload
    upload_ready_index = IndexStorage(encrypted_index, DOCUMENT_SIZE)

    # Upload each file and save the generated uri in the lookup table
    for file_data, callback in upload_ready_index:
        response = await client.room_send(
            room_id=DESTINATION_ROOM,
            message_type="m.room.message",
            content={
                "msgtype": "m.text",
                "body": json.dumps(file_data)
            },
        )
        callback(response.event_id)

    # Update the lookup table
    upload_ready_index.update_lookup_table()
    lookup_table = upload_ready_index.lookup_table

    # Cleanup
    del encrypted_index, upload_ready_index

    return lookup_table


async def search_in_index(
    client: AsyncClient,
    index: EncryptedSearch,
    query: str,
) -> Set[str]:
    """Searches for events in `index` which contain keywords from `query`.

    Args:
        client: Authenticated Nio client
        index: Searchable index provided by library
        query: Search query string

    Returns:
        Set of event ids pointing to events in `SOURCE_ROOM` which contain keywords from the provided `query`.
    """

    # Lookup the query in the index
    event_ids = index.lookup(query)

    # Fetch files from room
    fetched_files = {}
    for event_id in event_ids:
        response = await client.room_get_event(DESTINATION_ROOM, event_id)
        fetched_files[event_id] = json.loads(
            response.event.source["content"]["body"])

    # Locate message ids
    search_result = index.locate(fetched_files)
    return search_result


async def display_results(
    client: AsyncClient,
    query: str,
    results: Set[str],
):
    """Displays contents of messages whose ids are in `results`.

    Args:
        client: Authenticated Nio client
        query: Search query string
        results: Set of search results provided by library
    """

    print("=" * 10 + query + "=" * 10)
    for event_id in results:
        response = await client.room_get_event(SOURCE_ROOM, event_id)
        print(">" * 10 + event_id + ">" * 10)
        print("~" * 10 + response.event.sender + "~" * 10)
        print(response.event.source["content"]["body"])
        print("<" * 10 + event_id + "<" * 10)
        print('\n' * 3)
    print('\n' * 3)


async def clear_destination_room(
    client: AsyncClient,
    lookup_tables: Iterable[LookupTable],
):
    """Clears messages from `DESTINATION_ROOM`.

    Args:
        client: Authenticated Nio client
        lookup_tables: Iterable of lookup tables to purge
    """

    events_to_delete = set()
    for lookup_table in lookup_tables:
        for locations in lookup_table.values():
            for location in locations:
                events_to_delete.add(location.mxc_uri)

    for event_id in events_to_delete:
        await client.room_redact(
            DESTINATION_ROOM,
            event_id,
            reason="Cleanup after example completion",
            tx_id=event_id,
        )


async def main():
    """Calls the methods defined above to demonstrate an implementation of encrypted-search."""

    client, token = await login()
    batch_1, batch_2 = await fetch_n1_plus_n2_messages(client, token)
    lookup_table_1 = await create_and_upload_index(client, batch_1)
    lookup_table_2 = await create_and_upload_index(client, batch_2)

    index = EncryptedSearch((lookup_table_1, lookup_table_2))
    for query in SEARCH_QUERIES:
        results = await search_in_index(client, index, query)
        await display_results(client, query, results)

    await clear_destination_room(client, (lookup_table_1, lookup_table_2))

    await client.close()


if __name__ == '__main__':
    asyncio.run(main())
