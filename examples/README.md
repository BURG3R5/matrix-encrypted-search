# Matrix-Encrypted-Search Examples

Files in this directory showcase the capabilities of `matrix-encrypted-search` using [the `matrix-nio` client](https://github.com/poljar/matrix-nio).

You can find installation instructions and the documentation for the examples on [the `matrix-encrypted-search` Wiki](https://github.com/BURG3R5/matrix-encrypted-search/wiki/Examples).

It is recommended that you work through the examples *in a specific order*:
1. [`basic_index.py`](basic_index.py): demonstrates simple, straight-forward use of the library.
   - Indexes the latest N messages from a given room and searches through them.
2. [`multiple_indices.py`](multiple_indices.py): demonstrates the ability to search through several indices at once.
   - Indexes two indices for a room and searches through them together.
3. [`merging_indices.py`](merging_indices.py): demonstrates the housekeeping step of merging two indices of a room into one.
   - Indexes two indices for a room, then merges them into a single index and searches through that.
