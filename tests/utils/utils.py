import random
import string

INVALID_CHARS = r"/\;,.<>|?*"


def random_lower_string(length: int = 32, invalid_count: int = 0) -> str:
    result = "".join(random.choices(string.ascii_lowercase, k=length))
    for _ in range(invalid_count):
        invalid_char = random.choice(INVALID_CHARS)
        insert_pos = random.randint(0, len(result) - 1)
        result = result[:insert_pos] + invalid_char + result[insert_pos:]
    return result
