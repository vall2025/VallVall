from utility.library import *


def validate_input(user_id, auth_Code, secret_key):
    """
    Validate input parameters to ensure they meet basic criteria.
    """
    missing_fields = []
    if not user_id:
        missing_fields.append("user_id")
    if not auth_Code:
        missing_fields.append("auth_Code")
    if not secret_key:
        missing_fields.append("secret_key")

    if missing_fields:
        raise ValueError(
            f"{', '.join(missing_fields)} {'is' if len(missing_fields) == 1 else 'are'} required and cannot be empty.")


def generate_checksum(user_id, auth_Code, secret_key):
    """
    Generate SHA-256 checksum using userId, authCode, and apiSecret.
    """
    try:
        # Validate inputs
        validate_input(user_id, auth_Code, secret_key)

        # Concatenate the inputs securely
        data = f"{user_id}{auth_Code}{secret_key}"

        # Generate SHA-256 hash
        checksum = hashlib.sha256(data.encode('utf-8')).hexdigest()
        print(checksum)

        return checksum

    except ValueError as e:
        raise ValueError(e)

    except Exception as e:
        raise Exception(f"generate_checksum: {e}")
