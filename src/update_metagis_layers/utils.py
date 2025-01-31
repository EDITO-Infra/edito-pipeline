import re

def custom_slugify(text, separator='_'):
    """
    Create a slugified version of a text string.

    :param text: The input text to be slugified.
    :type text: str
    :param separator: The separator to replace spaces and special characters.
    :type separator: str, optional
    :return: The slugified text.
    :rtype: str
    """
    # Remove leading dots and spaces
    text = re.sub(r'^[.\s]+', '', text)

    # remove trailing dots and spaces
    text = re.sub(r'[.\s]+$', '', text)
    #specifically replace / with underscore
    text = re.sub(r'/', '_', text)

    # Replace special characters with separator
    text = re.sub(r'[-\s]+', separator, text)

    # Replace multiple separator with separator
    text = re.sub(r'__', separator, text)

    # Replace dots followed by a digit with separator
    text = re.sub(r'\.(\d)', r'.\1', text)

    # Remove other non-alphanumeric characters
    text = re.sub(r'[^\w\s.-]', '', text)

    #remove exponents
    text = re.sub(r'²', '_2', text)
    text = re.sub(r'³', '_3', text)

    return text.lower()