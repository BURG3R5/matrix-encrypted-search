# Matrix Encrypted Search

A Python library with methods to create, update and search on encrypted indices stored in content repositories in a Matrix homeserver.

GSoC '22 project with Matrix.org: [Project link](https://summerofcode.withgoogle.com/programs/2022/projects/xjCmlvMW)

## Overview

The library has four main modules:

##### Index

[index.py](https://github.com/BURG3R5/matrix-encrypted-search/blob/main/encrypted_search/index.py) contains the `EncryptedIndex` class, which is used to convert a list of room events into a structurally encrypted index according to the aforementioned SSE scheme. The index created thusly consists of two data structures: a lookup table and a datastore.

The lookup table is a mapping from keywords to locations in the datastore where their results can be found. Meanwhile, the datastore is a multi-dimensional array which contains random batches of event ids for events in which the keywords occur.

##### Storage

[storage.py](https://github.com/BURG3R5/matrix-encrypted-search/blob/main/encrypted_search/storage.py) contains the `IndexStorage` class, which is used to transform the single massive datastore into smaller blobs that can be stored as files on a Matrix homeserver. It exposes an iterator for the client to store the blobs on the server and then updates the lookup table of the index to point to remote locations using the MXC URIs instead of local array positions.

##### Merge

[merge.py](https://github.com/BURG3R5/matrix-encrypted-search/blob/main/encrypted_search/merge.py) contains the `IndexMerge` class, which is used to merge two or more remote encrypted indices. It exposes an iterator for the client to fetch data blobs from one or more homeservers and merges them into a new encrypted index.

##### Search

[search.py](https://github.com/BURG3R5/matrix-encrypted-search/blob/main/encrypted_search/search.py) contains the `EncryptedSearch` class, which is used to search for a query across one or more encrypted indices at once. It performs this operation in two steps: First identifying the files to be fetched, then locating the relevant chunks within those files.
