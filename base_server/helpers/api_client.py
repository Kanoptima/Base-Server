""" Module to provide ApiClient that handles all API requests throughout application. """

import logging
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import Timeout, HTTPError, RequestException, JSONDecodeError
from urllib3.util.retry import Retry

from base_server.helpers.dates import current_iso_timestamp

logger = logging.getLogger(__name__)


class ApiClient:
    """ Class providing generalised API functionality. """

    def __init__(self, base_url: Optional[str] = None, default_headers: Optional[dict] = None, retries=3, timeout=30):
        if isinstance(base_url, str):
            self.base_url = base_url.rstrip("/")
        else:
            self.base_url = None
        self.default_headers = default_headers or {}
        self.timeout = timeout

        # Splunk logging configuration
        self.splunk_log_url = None
        self.splunk_log_headers = None

        # Set up session with retries
        self.session = requests.Session()
        retry_strategy = Retry(
            total=retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504, 204],
            allowed_methods=['GET', 'POST', 'PUT', 'DELETE'],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def _prepare_url(self, endpoint: str):
        if self.base_url:
            return f'{self.base_url}/{endpoint.lstrip('/')}'
        return endpoint.lstrip('/')

    def setup_splunk_logging(self, url: str, headers: dict):
        """Sets up splunk logging for this API client, so future requests that have a destination provided will be logged in splunk.

        Args:
            url (str): URL to send logs to.
            headers (dict): Headers to use when sending logs to Splunk, must contain valid authentication headers.
        """
        self.splunk_log_url = url
        self.splunk_log_headers = headers

    def log_response(self, response: requests.Response, destination: str):
        """Logs an API response to Splunk, sending the code and raw content of the response.

        Args:
            response (requests.Response): Response object from the request.
            destination (str): Identifier for the destination of the request, used in logging.
        """
        if not self.splunk_log_url or not self.splunk_log_headers:
            logger.warning('Splunk logging is not configured.')
            return

        data = f'[{current_iso_timestamp()}] data_ingest {destination} {response.status_code} {response.content.decode()}'
        try:
            self.session.post(
                self.splunk_log_url,
                headers=self.splunk_log_headers,
                data=data,
                timeout=self.timeout,
            )
        except RequestException as e:
            logger.error('Failed to log response to Splunk: %s', e)

    def log_success(self, success: bool, destination: str):
        """Logs API success to Splunk, where raw content is not applicable.

        Args:
            success (bool): Status of the request.
            destination (str): Identifier for the destination of the request, used in logging.
        """
        if not self.splunk_log_url or not self.splunk_log_headers:
            logger.warning('Splunk logging is not configured.')
            return

        if success:
            data = f'[{current_iso_timestamp()}] data_ingest {destination} success'
        else:
            data = f'[{current_iso_timestamp()}] data_ingest {destination} failure'
        try:
            self.session.post(
                self.splunk_log_url,
                headers=self.splunk_log_headers,
                data=data,
                timeout=self.timeout,
            )
        except RequestException as e:
            logger.error('Failed to log response to Splunk: %s', e)

    def request(self, method: str, endpoint: str, headers: Optional[dict] = None, destination: Optional[str] = None,
                handle_400=False, raw=False, **kwargs) -> Any:
        """Makes a request to the specified endpoint with the given method and headers.

        Args:
            method (str): GET, POST, PUT, DELETE etc.
            endpoint (str): Endpoint to make the request to, relative to the base URL if set.
            headers (Optional[dict], optional): Headers to use in the request. Defaults to set headers.
            destination (Optional[str], optional): If set, will log the response as going to the destination set here.
            handle_400 (bool, optional): Whether to treat a 400 response as a non error state, specifically useful if expecting not finding something
                as part of normal operation. Defaults to False.
            raw (bool, optional): Whether to return the raw bytes of the response instead of attempting to parse json. Defaults to False.
            **kwargs: Additional options. Supported keys include:
                - params (dict): Request parameters to be sent in the URL.
                - data (str): Data to be sent in the body of the request.
                - json (dict): JSON data to be sent in the body of the request.

        Returns:
            Any: If raw is True, returns the raw bytes of the response, otherwise attempts to return parsed JSON response. None on error.
        """
        url = self._prepare_url(endpoint)
        headers = {**self.default_headers, **(headers or {})}

        response: Optional[requests.Response] = None
        try:
            response = self.session.request(
                method, url, headers=headers, timeout=self.timeout, **kwargs
            )
            response.raise_for_status()
            if raw:
                return response.content
            return response.json()

        except Timeout:
            logger.error('Request to %s timed out.', url)

        except HTTPError as e:
            if response is None:
                logger.error('No response on HTTPError for %s: %s', url, e)
                return None

            if handle_400 and response.status_code == 400:
                logger.info(
                    'Bad request accepted as having meaning (probably non existent annature envelope) for %s: %s', url, e)
                return {'message': 'Bad request'}

            # Try to extract error message
            try:
                error_json = response.json()
                if "messages" in error_json:
                    for msg in error_json["messages"]:
                        logger.error('API error for %s: %s - %s',
                                     url, msg.get('type'), msg.get('text'))
                else:
                    logger.error("HTTP error for %s: %s", url, e)
                    logger.error("Response JSON: %s", error_json)
            except JSONDecodeError:
                logger.error("HTTP error for %s: %s", url, e)
                logger.error("Raw response: %s", response.text)

        except RequestException as e:
            logger.error('Request exception for %s: %s', url, e)
        finally:
            if destination:
                if self.splunk_log_headers and self.splunk_log_url and response:
                    self.log_response(response, destination)
                else:
                    self.log_success(False, destination)

        return None

    def get(self, endpoint: str, headers: Optional[dict] = None, destination: Optional[str] = None, handle_400=False, raw=False, **kwargs):
        """Makes a GET request to the specified endpoint with the given headers.

        Args:
            endpoint (str): Endpoint to make the request to, relative to the base URL if set.
            headers (Optional[dict], optional): Headers to use in the request. Defaults to set headers.
            destination (Optional[str], optional): If set, will log the response as going to the destination set here.
            handle_400 (bool, optional): Whether to treat a 400 response as a non error state, specifically useful if expecting not finding something
                as part of normal operation. Defaults to False.
            raw (bool, optional): Whether to return the raw bytes of the response instead of attempting to parse json. Defaults to False.
            **kwargs: Additional options. Supported keys include:
                - params (dict): Request parameters to be sent in the URL.
                - data (str): Data to be sent in the body of the request.
                - json (dict): JSON data to be sent in the body of the request.

        Returns:
            Any: If raw is True, returns the raw bytes of the response, otherwise attempts to return parsed JSON response. None on error."""
        return self.request('GET', endpoint, headers=headers, destination=destination, handle_400=handle_400, raw=raw, **kwargs)

    def post(self, endpoint: str, headers: Optional[dict] = None, destination: Optional[str] = None, handle_400=False, raw=False, **kwargs):
        """Makes a POST request to the specified endpoint with the given headers.

        Args:
            endpoint (str): Endpoint to make the request to, relative to the base URL if set.
            headers (Optional[dict], optional): Headers to use in the request. Defaults to set headers.
            destination (Optional[str], optional): If set, will log the response as going to the destination set here.
            handle_400 (bool, optional): Whether to treat a 400 response as a non error state, specifically useful if expecting not finding something
                as part of normal operation. Defaults to False.
            raw (bool, optional): Whether to return the raw bytes of the response instead of attempting to parse json. Defaults to False.
            **kwargs: Additional options. Supported keys include:
                - params (dict): Request parameters to be sent in the URL.
                - data (str): Data to be sent in the body of the request.
                - json (dict): JSON data to be sent in the body of the request.

        Returns:
            Any: If raw is True, returns the raw bytes of the response, otherwise attempts to return parsed JSON response. None on error."""
        return self.request('POST', endpoint, headers=headers, destination=destination, handle_400=handle_400, raw=raw, **kwargs)

    def put(self, endpoint: str, headers: Optional[dict] = None, destination: Optional[str] = None, handle_400=False, raw=False, **kwargs):
        """Makes a PUT request to the specified endpoint with the given headers.

        Args:
            endpoint (str): Endpoint to make the request to, relative to the base URL if set.
            headers (Optional[dict], optional): Headers to use in the request. Defaults to set headers.
            destination (Optional[str], optional): If set, will log the response as going to the destination set here.
            handle_400 (bool, optional): Whether to treat a 400 response as a non error state, specifically useful if expecting not finding something
                as part of normal operation. Defaults to False.
            raw (bool, optional): Whether to return the raw bytes of the response instead of attempting to parse json. Defaults to False.
            **kwargs: Additional options. Supported keys include:
                - params (dict): Request parameters to be sent in the URL.
                - data (str): Data to be sent in the body of the request.
                - json (dict): JSON data to be sent in the body of the request.

        Returns:
            Any: If raw is True, returns the raw bytes of the response, otherwise attempts to return parsed JSON response. None on error."""
        return self.request('PUT', endpoint, headers=headers, destination=destination, handle_400=handle_400, raw=raw, **kwargs)

    def delete(self, endpoint: str, headers: Optional[dict] = None, destination: Optional[str] = None, handle_400=False, raw=False, **kwargs):
        """Makes a DELETE request to the specified endpoint with the given headers.

        Args:
            endpoint (str): Endpoint to make the request to, relative to the base URL if set.
            headers (Optional[dict], optional): Headers to use in the request. Defaults to set headers.
            destination (Optional[str], optional): If set, will log the response as going to the destination set here.
            handle_400 (bool, optional): Whether to treat a 400 response as a non error state, specifically useful if expecting not finding something
                as part of normal operation. Defaults to False.
            raw (bool, optional): Whether to return the raw bytes of the response instead of attempting to parse json. Defaults to False.
            **kwargs: Additional options. Supported keys include:
                - params (dict): Request parameters to be sent in the URL.
                - data (str): Data to be sent in the body of the request.
                - json (dict): JSON data to be sent in the body of the request.

        Returns:
            Any: If raw is True, returns the raw bytes of the response, otherwise attempts to return parsed JSON response. None on error."""
        return self.request('DELETE', endpoint, headers=headers, destination=destination, handle_400=handle_400, raw=raw, **kwargs)
