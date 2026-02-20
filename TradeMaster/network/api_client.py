from utility.library import *


class RequestHandler:
    def __init__(self, session_token: str):
        """
        Initializes the request handler with a session token.

        Args:
            session_token (str): Authorization token for API calls.
        """
        self.headers = {
            "Authorization": session_token
        }

    def request(self, url: str, method: str, data=None, params=None) -> dict:
        """
        Sends an API request and handles the response.

        Args:
            url (str): Endpoint URL.
            method (str): API method - GET, POST, PUT, DELETE.
            data (dict or str, optional): Request payload.
            params (dict, optional): Query parameters.

        Returns:
            dict: Parsed JSON response or error structure.
        """
        try:
            method = method.upper()
            kwargs = {
                "headers": self.headers,
            }

            if isinstance(params, dict):
                kwargs["params"] = params

            if data is not None:
                kwargs["json"] = data

            if method == "POST":
                response = requests.post(url, timeout=20, **kwargs)
            elif method == "GET":
                response = requests.get(url, timeout=20, **kwargs)
            elif method == "PUT":
                response = requests.put(url, timeout=20, **kwargs)
            elif method == "DELETE":
                response = requests.delete(url, timeout=20, **kwargs)
            else:
                raise ValueError(f"Unsupported request type: {method}")

            return self._handle_response(response)

        except (requests.ConnectionError, requests.Timeout) as conn_exc:
            return {
                'stat': 'Not_ok',
                'emsg': str(conn_exc),
                'encKey': None
            }

        except ValueError as ve:
            return {
                'stat': 'Not_ok',
                'emsg': f"Error: {str(ve)}",
                'encKey': None
            }

        except Exception as e:
            return {
                'stat': 'Not_ok',
                'emsg': f"Unexpected Error: {str(e)}",
                'encKey': None
            }

    def _handle_response(self, response: requests.Response) -> dict:
        """
        Parses the response object and returns a structured result.

        Args:
            response (requests.Response): Response from the request.

        Returns:
            dict: Parsed response or error structure.
        """
        if response.status_code == 200:
            return response.json()
        else:
            return {
                'stat': 'Not_ok',
                'emsg': f"{response.status_code} - {response.reason}",
                'encKey': None
            }
