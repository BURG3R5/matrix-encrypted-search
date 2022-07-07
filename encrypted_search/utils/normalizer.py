from typing import Set

from nltk.corpus import stopwords
from nltk.tokenize.regexp import RegexpTokenizer, WhitespaceTokenizer


def normalize(string: str) -> Set[str]:
    """Tokenizes and normalizes a given string

    Args:
        string: Raw string to be tokenized

    Returns:
        A set of normalized tokens

    """

    # Initialize helper methods
    split_by_whitespaces = WhitespaceTokenizer().tokenize
    get_letter_sequences = RegexpTokenizer(pattern=r"\w+").tokenize
    strip_punctuation = lambda x: x.strip("""<>()[]{}'"_.,;:!?$%&-*~^/\\""")

    # Lowercase
    string = string.lower()

    # Split by whitespaces
    temp_tokens = set(split_by_whitespaces(string))

    tokens: Set[str] = set()
    for token in temp_tokens:
        # Strip punctuation
        stripped_token = strip_punctuation(token)
        tokens.add(stripped_token)

        # Find sub-tokens
        sub_tokens = set(get_letter_sequences(stripped_token))
        tokens |= sub_tokens

    # Remove stopwords
    tokens -= set(stopwords.words("english"))

    return tokens - {''}


def normalize_surface(string: str) -> Set[str]:
    """Tokenizes and normalizes a given string without breaking down to sub-tokens.

    Args:
        string: Raw string to be tokenized

    Returns:
        A set of normalized tokens

    """

    # Initialize helper methods
    split_by_whitespaces = WhitespaceTokenizer().tokenize
    strip_punctuation = lambda x: x.strip("""<>()[]{}'"_.,;:!?$%&-*~^/\\""")

    # Lowercase
    string = string.lower()

    # Split by whitespaces
    temp_tokens = set(split_by_whitespaces(string))

    # Remove stopwords
    stopwords_set = set(stopwords.words("english"))
    stopwords_set.add('')

    tokens = {strip_punctuation(token)
              for token in temp_tokens} - stopwords_set

    return tokens
