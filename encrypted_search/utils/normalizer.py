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
        striped_token = strip_punctuation(token)
        tokens.add(striped_token)

        # Find sub-tokens
        sub_tokens = set(get_letter_sequences(striped_token))
        tokens |= sub_tokens

    # Remove stopwords
    tokens -= set(stopwords.words("english"))

    return tokens - {''}
