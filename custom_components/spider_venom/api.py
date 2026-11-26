"""Client for Spider Grills Venom AWS IoT shadows."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import contextlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import hmac
import json
import logging
import re
import socket
import ssl
import threading
from typing import Any
from urllib.parse import quote
import uuid

from aiohttp import ClientConnectorError, ClientError, ClientSession, ClientTimeout
import paho.mqtt.client as mqtt

_LOGGER = logging.getLogger(__name__)
DEFAULT_TIMEOUT = ClientTimeout(total=15)
_MQTT_TIMEOUT = 15
_MQTT_CONNECT_TIMEOUT = 10.0
_INITIAL_RECONNECT_BACKOFF = 2.0
_MAX_RECONNECT_BACKOFF = 60.0
_MAX_DEBUG_LOG_CHARS = 4096
_MAX_NON_JSON_LOG_CHARS = 500
_REDACTED_KEYS = {
    "accesskeyid",
    "accesstoken",
    "apikey",
    "authorization",
    "cookie",
    "email",
    "identityid",
    "idtoken",
    "jwt",
    "password",
    "refreshtoken",
    "secret",
    "secretaccesskey",
    "secretkey",
    "sessiontoken",
}


class SpiderVenomApiError(Exception):
    """Raised when the Spider Grills cloud API cannot be reached."""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class SpiderVenomAuthError(SpiderVenomApiError):
    """Raised when Spider Grills authentication fails."""


@dataclass(slots=True)
class _AttemptState:
    """Per-connection MQTT handshake and disconnect state."""

    subscribe_event: asyncio.Event
    disconnect_event: asyncio.Event
    subscribe_error: tuple[str, int | None] | None = None
    disconnect_error: tuple[str, int | None] | None = None

    def on_connect(self, mqtt_client: mqtt.Client, topics: list[tuple[str, int]], reason_code: Any) -> None:
        if reason_code.is_failure:
            reason_str = str(reason_code)
            self.subscribe_error = (f"MQTT connect refused: {reason_str}", _auth_status(reason_str))
            self.subscribe_event.set()
            return
        mqtt_client.subscribe(topics)

    def on_subscribe(self, reason_codes: Any) -> None:
        if any(code.is_failure for code in reason_codes):
            self.subscribe_error = (
                f"MQTT subscribe refused: {[str(c) for c in reason_codes]}",
                None,
            )
        self.subscribe_event.set()

    def on_disconnect(self, reason_code: Any) -> None:
        reason_str = str(reason_code)
        self.disconnect_error = (f"MQTT disconnected: {reason_str}", _auth_status(reason_str))
        self.disconnect_event.set()
        self.subscribe_event.set()


@dataclass(slots=True)
class _Credentials:
    access_key_id: str
    secret_key: str
    session_token: str
    expiration: datetime
    identity_id: str

    @property
    def expires_soon(self) -> bool:
        """Return true when credentials should be refreshed."""
        return self.expiration <= datetime.now(UTC) + timedelta(minutes=5)


class SpiderVenomClient:
    """AWS Cognito + IoT MQTT-over-WebSocket client for Venom shadows."""

    def __init__(
        self,
        *,
        session: ClientSession,
        endpoint: str,
        identity_pool_id: str,
        region: str,
        thing_name: str,
        timeout: ClientTimeout = DEFAULT_TIMEOUT,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self._session = session
        self._endpoint = endpoint.removeprefix("https://").removeprefix("http://").strip("/")
        self._identity_pool_id = identity_pool_id
        self._region = region
        self._thing_name = thing_name
        self._timeout = timeout
        self._ssl_context = ssl_context
        self._ssl_context_lock = threading.Lock()
        self._credentials: _Credentials | None = None
        self._identity_id: str | None = None
        self._credentials_lock = asyncio.Lock()

        # Persistent MQTT push state
        self._on_shadow_update: Callable[[dict[str, Any]], None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._runner_task: asyncio.Task[None] | None = None
        self._wake_event: asyncio.Event | None = None
        self._initial_ready_event: asyncio.Event | None = None
        self._initial_error: SpiderVenomApiError | None = None
        self._stopping = False
        self._ready = False
        self._mqtt_client: mqtt.Client | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}

    async def async_get_shadow(self) -> dict[str, Any]:
        """Fetch the current AWS IoT thing shadow."""
        return await self._async_iot_shadow_request(action="get", payload={})

    async def async_set_target_temperature(self, temperature: int) -> dict[str, Any]:
        """Set the Venom target temperature."""
        return await self.async_update_desired({"heat": {"t2": {"trgt": temperature}}})

    async def async_update_desired(self, desired: dict[str, Any]) -> dict[str, Any]:
        """Update desired shadow state."""
        return await self._async_iot_shadow_request(
            action="update",
            payload={"state": {"desired": desired}},
        )

    async def async_test_connection(self) -> dict[str, Any]:
        """Fetch a shadow once to validate config."""
        return await self.async_get_shadow()

    async def async_start(self, on_shadow_update: Callable[[dict[str, Any]], None]) -> None:
        """Start the persistent MQTT-over-WebSocket connection and wait until subscribed."""
        self._on_shadow_update = on_shadow_update
        if self._runner_task is not None and not self._runner_task.done():
            return

        self._loop = asyncio.get_running_loop()
        self._stopping = False
        self._wake_event = asyncio.Event()
        self._initial_ready_event = asyncio.Event()
        self._initial_error = None
        self._runner_task = asyncio.create_task(
            self._async_connection_loop(),
            name=f"spider_venom_mqtt_{self._thing_name}",
        )

        try:
            async with asyncio.timeout(_MQTT_TIMEOUT):
                await self._initial_ready_event.wait()
        except TimeoutError as err:
            await self.async_stop()
            raise SpiderVenomApiError(
                f"IoT MQTT connection timed out after {_MQTT_TIMEOUT}s"
            ) from err

        if self._initial_error is not None:
            err = self._initial_error
            await self.async_stop()
            raise err

    async def async_stop(self) -> None:
        """Stop the persistent MQTT-over-WebSocket connection cleanly."""
        self._stopping = True
        self._ready = False
        if self._wake_event is not None:
            self._wake_event.set()

        runner = self._runner_task
        self._runner_task = None
        if runner is not None and not runner.done():
            try:
                async with asyncio.timeout(5):
                    await runner
            except TimeoutError:
                runner.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await runner

        self._fail_pending(SpiderVenomApiError("IoT MQTT client stopped"))

    def _get_ssl_context(self) -> ssl.SSLContext:
        """Return a cached SSL context, creating it lazily off the event loop."""
        if self._ssl_context is None:
            with self._ssl_context_lock:
                if self._ssl_context is None:
                    self._ssl_context = ssl.create_default_context()
        return self._ssl_context

    def _connect_and_start_client(self, client: mqtt.Client) -> None:
        """Connect an MQTT client over TLS/WebSocket and start its network loop."""
        try:
            client.connect(self._endpoint, 443, keepalive=30)
        except (OSError, ssl.SSLError) as err:
            err_msg = str(err)
            err_lower = err_msg.lower()
            err_status = (
                403
                if isinstance(err, mqtt.WebsocketConnectionError)
                or any(
                    marker in err_lower
                    for marker in ("401", "403", "forbidden", "unauthorized")
                )
                else None
            )
            raise SpiderVenomApiError(
                f"IoT MQTT could not connect to {self._endpoint}: {err_msg}",
                status=err_status,
            ) from err
        client.loop_start()

    def _teardown_client(self, client: mqtt.Client) -> None:
        """Disconnect an MQTT client and join its background network thread."""
        with contextlib.suppress(Exception):
            client.disconnect()
        with contextlib.suppress(Exception):
            client.loop_stop()

    def _fail_pending(self, err: SpiderVenomApiError) -> None:
        """Reject any in-flight shadow requests."""
        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(err)
        self._pending.clear()

    def _handle_push_message(self, doc: dict[str, Any]) -> None:
        """Deliver an incoming shadow/update/documents update to the coordinator."""
        current = doc.get("current")
        if not isinstance(current, dict):
            return
        shadow_doc = dict(current)
        if "timestamp" not in shadow_doc and "timestamp" in doc:
            shadow_doc["timestamp"] = doc["timestamp"]
        if self._on_shadow_update is not None:
            self._on_shadow_update(shadow_doc)

    def _resolve_pending(
        self,
        action: str,
        token: str,
        doc: dict[str, Any],
        raw: str,
        is_rejected: bool,
    ) -> None:
        """Resolve or reject an awaiting shadow request by clientToken."""
        future = self._pending.get(token)
        if future is None or future.done():
            return
        if is_rejected:
            code = doc.get("code")
            err_status = code if isinstance(code, int) else 400
            _log_response_body(f"IoT shadow {action}", err_status, raw)
            future.set_exception(
                SpiderVenomApiError(
                    f"Shadow {action} rejected: {code} {doc.get('message')}",
                    status=err_status,
                )
            )
        else:
            _log_response_body(f"IoT shadow {action}", 200, raw)
            future.set_result(doc)

    async def _async_connection_loop(self) -> None:
        """Maintain a persistent MQTT-over-WebSocket connection with credential rotation."""
        loop = asyncio.get_running_loop()
        backoff = _INITIAL_RECONNECT_BACKOFF
        connected_once = False
        auth_retried_initial = False
        shadow_prefix = f"$aws/things/{self._thing_name}/shadow"
        topics = [
            (f"{shadow_prefix}/get/accepted", 1),
            (f"{shadow_prefix}/get/rejected", 1),
            (f"{shadow_prefix}/update/accepted", 1),
            (f"{shadow_prefix}/update/rejected", 1),
            (f"{shadow_prefix}/update/documents", 1),
        ]

        while not self._stopping:
            client: mqtt.Client | None = None
            state = _AttemptState(
                subscribe_event=asyncio.Event(),
                disconnect_event=asyncio.Event(),
            )

            def on_connect(
                mqtt_client: mqtt.Client,
                userdata: Any,
                flags: Any,
                reason_code: Any,
                properties: Any = None,
                *,
                attempt_state: _AttemptState = state,
            ) -> None:
                loop.call_soon_threadsafe(attempt_state.on_connect, mqtt_client, topics, reason_code)

            def on_subscribe(
                mqtt_client: mqtt.Client,
                userdata: Any,
                mid: Any,
                reason_codes: Any,
                properties: Any = None,
                *,
                attempt_state: _AttemptState = state,
            ) -> None:
                loop.call_soon_threadsafe(attempt_state.on_subscribe, reason_codes)

            def on_message(
                mqtt_client: mqtt.Client,
                userdata: Any,
                msg: mqtt.MQTTMessage,
            ) -> None:
                raw = msg.payload.decode("utf-8", errors="replace")
                try:
                    doc = json.loads(raw)
                except ValueError:
                    return
                if not isinstance(doc, dict):
                    return
                if msg.topic.endswith("/update/documents"):
                    _log_response_body("IoT shadow push", 200, raw)
                    loop.call_soon_threadsafe(self._handle_push_message, doc)
                    return
                req_token = doc.get("clientToken")
                if not isinstance(req_token, str):
                    return
                action = "get" if "/shadow/get/" in msg.topic else "update"
                is_rejected = msg.topic.endswith("/rejected")
                loop.call_soon_threadsafe(
                    self._resolve_pending, action, req_token, doc, raw, is_rejected
                )

            def on_disconnect(
                mqtt_client: mqtt.Client,
                userdata: Any,
                flags: Any,
                reason_code: Any,
                properties: Any = None,
                *,
                attempt_state: _AttemptState = state,
            ) -> None:
                loop.call_soon_threadsafe(attempt_state.on_disconnect, reason_code)

            try:
                credentials = await self._async_get_credentials()
                ws_path = self._presigned_ws_path(credentials)
                ssl_ctx = await loop.run_in_executor(None, self._get_ssl_context)

                client = mqtt.Client(
                    mqtt.CallbackAPIVersion.VERSION2,
                    client_id=str(uuid.uuid4()),
                    transport="websockets",
                    reconnect_on_failure=False,
                )
                client.connect_timeout = _MQTT_CONNECT_TIMEOUT
                client.ws_set_options(path=ws_path)
                client.tls_set_context(ssl_ctx)
                client.on_connect = on_connect
                client.on_subscribe = on_subscribe
                client.on_message = on_message
                client.on_disconnect = on_disconnect

                await loop.run_in_executor(None, self._connect_and_start_client, client)

                async with asyncio.timeout(_MQTT_TIMEOUT):
                    await state.subscribe_event.wait()

                if state.subscribe_error is not None:
                    raise SpiderVenomApiError(state.subscribe_error[0], status=state.subscribe_error[1])
                if state.disconnect_event.is_set():
                    msg, status = state.disconnect_error or ("MQTT disconnected during connect", None)
                    raise SpiderVenomApiError(msg, status=status)

                self._mqtt_client = client
                self._ready = True
                backoff = _INITIAL_RECONNECT_BACKOFF
                connected_once = True
                if self._initial_ready_event is not None:
                    self._initial_ready_event.set()

                rotation_delay = max(
                    0.0,
                    (credentials.expiration - timedelta(minutes=5) - datetime.now(UTC)).total_seconds(),
                )
                wake_event = self._wake_event
                assert wake_event is not None
                disc_task = asyncio.create_task(state.disconnect_event.wait())
                wake_task = asyncio.create_task(wake_event.wait())
                try:
                    _done, pending_waits = await asyncio.wait(
                        {disc_task, wake_task},
                        timeout=rotation_delay,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for task in pending_waits:
                        task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await task
                except BaseException:
                    for task in (disc_task, wake_task):
                        task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await task
                    raise

                if self._stopping:
                    break

                if state.disconnect_event.is_set():
                    msg, status = state.disconnect_error or ("MQTT disconnected", None)
                    raise SpiderVenomApiError(msg, status=status)

                # Credential rotation threshold reached; reconnect proactively with fresh credentials.
                self._credentials = None

            except TimeoutError:
                err = SpiderVenomApiError(f"IoT MQTT subscribe timed out after {_MQTT_TIMEOUT}s")
                if not connected_once:
                    self._initial_error = err
                    if self._initial_ready_event is not None:
                        self._initial_ready_event.set()
                    break
                _LOGGER.warning("Spider Venom MQTT timed out, reconnecting in %.0fs", backoff)
                await self._async_sleep_interruptible(backoff)
                backoff = min(backoff * 2, _MAX_RECONNECT_BACKOFF)
            except SpiderVenomApiError as err:
                if err.status in (401, 403):
                    self._credentials = None
                if not connected_once:
                    if err.status in (401, 403) and not auth_retried_initial and not self._stopping:
                        auth_retried_initial = True
                        continue
                    self._initial_error = err
                    if self._initial_ready_event is not None:
                        self._initial_ready_event.set()
                    break
                _LOGGER.warning(
                    "Spider Venom MQTT connection lost, reconnecting in %.0fs: %s",
                    backoff,
                    err,
                )
                await self._async_sleep_interruptible(backoff)
                backoff = min(backoff * 2, _MAX_RECONNECT_BACKOFF)
            finally:
                self._ready = False
                if self._mqtt_client is client:
                    self._mqtt_client = None
                self._fail_pending(SpiderVenomApiError("IoT MQTT connection closed"))
                if client is not None:
                    await loop.run_in_executor(None, self._teardown_client, client)

    async def _async_sleep_interruptible(self, delay: float) -> None:
        """Wait for delay seconds or until async_stop wakes the loop."""
        if self._wake_event is None or self._stopping:
            return
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(delay):
                await self._wake_event.wait()

    async def _async_iot_shadow_request(
        self, *, action: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """Call the AWS IoT thing shadow over MQTT-over-WebSocket.

        Uses the persistent connection when active, or opens a short-lived connection
        for standalone validation calls (such as config flow setup).
        """
        if self._ready and self._mqtt_client is not None and self._loop is not None:
            token = uuid.uuid4().hex
            message = json.dumps({**payload, "clientToken": token}, separators=(",", ":"))
            topic = f"$aws/things/{self._thing_name}/shadow/{action}"
            future: asyncio.Future[dict[str, Any]] = self._loop.create_future()
            self._pending[token] = future
            try:
                info = self._mqtt_client.publish(topic, message, qos=1)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    raise SpiderVenomApiError(f"IoT shadow {action} publish failed: {info.rc}")
                async with asyncio.timeout(_MQTT_TIMEOUT):
                    return await future
            except TimeoutError as err:
                raise SpiderVenomApiError(
                    f"IoT shadow {action} timed out after {_MQTT_TIMEOUT}s"
                ) from err
            finally:
                self._pending.pop(token, None)

        loop = asyncio.get_running_loop()
        for attempt in range(2):
            credentials = await self._async_get_credentials()
            token = uuid.uuid4().hex
            message = json.dumps({**payload, "clientToken": token}, separators=(",", ":"))
            ws_path = self._presigned_ws_path(credentials)
            try:
                return await loop.run_in_executor(
                    None,
                    self._mqtt_shadow_call,
                    ws_path,
                    action,
                    message,
                    token,
                )
            except SpiderVenomApiError as err:
                if err.status in (401, 403) and attempt == 0:
                    self._credentials = None
                    continue
                raise

        raise SpiderVenomApiError(f"IoT shadow {action} failed after credential refresh")

    def _mqtt_shadow_call(
        self, ws_path: str, action: str, message: str, token: str
    ) -> dict[str, Any]:
        topic = f"$aws/things/{self._thing_name}/shadow/{action}"
        done = threading.Event()
        result: dict[str, Any] = {}

        def finish(key: str, value: Any) -> None:
            if not done.is_set():
                result[key] = value
                done.set()

        def on_connect(
            mqtt_client: mqtt.Client,
            userdata: Any,
            flags: Any,
            reason_code: Any,
            properties: Any = None,
        ) -> None:
            if reason_code.is_failure:
                reason_str = str(reason_code)
                finish("error", (f"MQTT connect refused: {reason_str}", _auth_status(reason_str)))
                return
            mqtt_client.subscribe([(f"{topic}/accepted", 1), (f"{topic}/rejected", 1)])

        def on_subscribe(
            mqtt_client: mqtt.Client,
            userdata: Any,
            mid: Any,
            reason_codes: Any,
            properties: Any = None,
        ) -> None:
            if any(code.is_failure for code in reason_codes):
                finish("error", (f"MQTT subscribe refused: {[str(c) for c in reason_codes]}", None))
                return
            mqtt_client.publish(topic, message, qos=1)

        def on_message(
            mqtt_client: mqtt.Client,
            userdata: Any,
            msg: mqtt.MQTTMessage,
        ) -> None:
            raw = msg.payload.decode("utf-8", errors="replace")
            try:
                doc = json.loads(raw)
            except ValueError:
                return
            # accepted/rejected topics are shared with the vendor app; match our request.
            if not isinstance(doc, dict) or doc.get("clientToken") != token:
                return
            if msg.topic.endswith("/rejected"):
                code = doc.get("code")
                err_status = code if isinstance(code, int) else 400
                _log_response_body(f"IoT shadow {action}", err_status, raw)
                finish(
                    "error",
                    (f"Shadow {action} rejected: {code} {doc.get('message')}", err_status),
                )
            else:
                _log_response_body(f"IoT shadow {action}", 200, raw)
                finish("doc", doc)

        def on_disconnect(
            mqtt_client: mqtt.Client,
            userdata: Any,
            flags: Any,
            reason_code: Any,
            properties: Any = None,
        ) -> None:
            reason_str = str(reason_code)
            finish("error", (f"MQTT disconnected: {reason_str}", _auth_status(reason_str)))

        # AWS IoT policy only permits a dashed-UUID client id; Cognito identity IDs or
        # undashed hex UUIDs are disconnected before CONNACK.
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=str(uuid.uuid4()),
            transport="websockets",
            reconnect_on_failure=False,
        )
        client.connect_timeout = _MQTT_CONNECT_TIMEOUT
        client.ws_set_options(path=ws_path)
        client.tls_set_context(self._get_ssl_context())
        client.on_connect = on_connect
        client.on_subscribe = on_subscribe
        client.on_message = on_message
        client.on_disconnect = on_disconnect

        self._connect_and_start_client(client)
        try:
            if not done.wait(_MQTT_TIMEOUT):
                raise SpiderVenomApiError(f"IoT shadow {action} timed out after {_MQTT_TIMEOUT}s")
        finally:
            self._teardown_client(client)

        if "error" in result:
            err_msg, err_status = result["error"]
            raise SpiderVenomApiError(err_msg, status=err_status)
        return result["doc"]

    async def _async_get_credentials(self) -> _Credentials:
        if self._credentials is not None and not self._credentials.expires_soon:
            return self._credentials

        async with self._credentials_lock:
            if self._credentials is not None and not self._credentials.expires_soon:
                return self._credentials

            identity = self._identity_id
            if identity is None:
                identity_id = await self._async_cognito_post(
                    "GetId", {"IdentityPoolId": self._identity_pool_id}
                )
                identity = identity_id.get("IdentityId")
                if not isinstance(identity, str) or not identity:
                    raise SpiderVenomApiError("Cognito GetId response did not include IdentityId")
                self._identity_id = identity

            try:
                credential_response = await self._async_cognito_post(
                    "GetCredentialsForIdentity", {"IdentityId": identity}
                )
            except SpiderVenomApiError as err:
                if err.status in (400, 401, 403):
                    self._identity_id = None
                raise

            credentials = credential_response.get("Credentials")
            if not isinstance(credentials, dict):
                raise SpiderVenomApiError(
                    "Cognito GetCredentialsForIdentity response did not include credentials"
                )

            try:
                expiration = datetime.fromtimestamp(float(credentials["Expiration"]), UTC)
                self._credentials = _Credentials(
                    access_key_id=credentials["AccessKeyId"],
                    secret_key=credentials["SecretKey"],
                    session_token=credentials["SessionToken"],
                    expiration=expiration,
                    identity_id=identity,
                )
            except (KeyError, TypeError, ValueError) as err:
                raise SpiderVenomApiError("Cognito credentials response was incomplete") from err

            return self._credentials

    async def _async_cognito_post(self, target: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"https://cognito-identity.{self._region}.amazonaws.com/"
        headers = {
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": f"AWSCognitoIdentityService.{target}",
        }

        try:
            async with self._session.post(
                url,
                headers=headers,
                json=payload,
                timeout=self._timeout,
            ) as response:
                body = await response.text()
                status = response.status
                _log_response_body(f"Cognito {target}", status, body)
        except ClientConnectorError as err:
            raise SpiderVenomApiError(
                f"Cognito {target} could not connect in region {self._region}: {err}"
            ) from err
        except (TimeoutError, socket.gaierror) as err:
            raise SpiderVenomApiError(
                f"Cognito {target} timed out or DNS failed in region {self._region}: {err}"
            ) from err
        except ClientError as err:
            raise SpiderVenomApiError(f"Cognito {target} request failed: {err}") from err

        if status != 200:
            error_detail: str = body[:300]
            try:
                err_parsed = json.loads(body) if body else {}
            except json.JSONDecodeError:
                err_parsed = None
            if isinstance(err_parsed, dict) and "__type" in err_parsed:
                error_detail = str(err_parsed["__type"])
            raise SpiderVenomApiError(
                f"Cognito {target} returned HTTP {status}: {error_detail}",
                status=status,
            )

        try:
            parsed = json.loads(body)
        except json.JSONDecodeError as err:
            raise SpiderVenomApiError(f"Cognito {target} response was not JSON") from err

        if not isinstance(parsed, dict):
            raise SpiderVenomApiError(f"Cognito {target} response had an unexpected shape")
        return parsed

    def _presigned_ws_path(self, credentials: _Credentials) -> str:
        now = datetime.now(UTC)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        service = "iotdevicegateway"
        credential_scope = f"{date_stamp}/{self._region}/{service}/aws4_request"
        query = {
            "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
            "X-Amz-Credential": f"{credentials.access_key_id}/{credential_scope}",
            "X-Amz-Date": amz_date,
            "X-Amz-SignedHeaders": "host",
        }
        query_string = "&".join(
            f"{key}={quote(value, safe='')}" for key, value in sorted(query.items())
        )
        canonical_request = "\n".join(
            [
                "GET",
                "/mqtt",
                query_string,
                f"host:{self._endpoint}\n",
                "host",
                hashlib.sha256(b"").hexdigest(),
            ]
        )
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode()).hexdigest(),
            ]
        )
        signature = hmac.new(
            self._signing_key(credentials.secret_key, date_stamp, service),
            string_to_sign.encode(),
            hashlib.sha256,
        ).hexdigest()
        # AWS IoT expects the session token appended after signing, not signed.
        return (
            f"/mqtt?{query_string}&X-Amz-Signature={signature}"
            f"&X-Amz-Security-Token={quote(credentials.session_token, safe='')}"
        )

    def _signing_key(self, secret_key: str, date_stamp: str, service: str) -> bytes:
        key_date = hmac.new(("AWS4" + secret_key).encode(), date_stamp.encode(), hashlib.sha256).digest()
        key_region = hmac.new(key_date, self._region.encode(), hashlib.sha256).digest()
        key_service = hmac.new(key_region, service.encode(), hashlib.sha256).digest()
        return hmac.new(key_service, b"aws4_request", hashlib.sha256).digest()


class SpiderGrillsAccountClient:
    """Client for Spider Grills account/device discovery."""

    def __init__(
        self,
        *,
        session: ClientSession,
        base_url: str = "https://spidergrills.app",
        timeout: ClientTimeout = DEFAULT_TIMEOUT,
    ) -> None:
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    async def async_login(self, *, email: str, password: str) -> str:
        """Log in and return an access token."""
        response = await self._async_request(
            "POST",
            "/auth/login",
            json_payload={"email": email, "password": password},
        )
        if not isinstance(response, dict):
            raise SpiderVenomApiError("Login response had an unexpected shape")
        token = response.get("accessToken")
        if not isinstance(token, str) or not token:
            raise SpiderVenomApiError("Login response did not include an access token")
        return token

    async def async_get_devices(self, access_token: str) -> list[dict[str, Any]]:
        """Fetch devices for the logged-in account."""
        response = await self._async_request("GET", "/api/devices", access_token=access_token)
        if isinstance(response, list):
            devices = response
        elif isinstance(response, dict) and isinstance(response.get("data"), list):
            devices = response["data"]
        elif isinstance(response, dict) and isinstance(response.get("devices"), list):
            devices = response["devices"]
        else:
            raise SpiderVenomApiError("Devices response had an unexpected shape")

        return [device for device in devices if isinstance(device, dict)]

    async def _async_request(
        self,
        method: str,
        path: str,
        *,
        json_payload: dict[str, Any] | None = None,
        access_token: str | None = None,
    ) -> Any:
        headers = {"Accept": "application/json"}
        if json_payload is not None:
            headers["Content-Type"] = "application/json"
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"

        try:
            async with self._session.request(
                method,
                f"{self._base_url}{path}",
                headers=headers,
                json=json_payload,
                timeout=self._timeout,
            ) as response:
                body = await response.text()
                status = response.status
                _log_response_body(
                    f"Spider Grills API {method} {path}",
                    status,
                    body,
                    redact_non_json=(path == "/auth/login"),
                )
        except ClientConnectorError as err:
            raise SpiderVenomApiError(f"Spider Grills API could not connect: {err}") from err
        except (TimeoutError, socket.gaierror) as err:
            raise SpiderVenomApiError(f"Spider Grills API timed out or DNS failed: {err}") from err
        except ClientError as err:
            raise SpiderVenomApiError(f"Spider Grills API request failed: {err}") from err

        if status in (401, 403):
            raise SpiderVenomAuthError(
                f"Spider Grills API returned HTTP {status}",
                status=status,
            )
        if status < 200 or status >= 300:
            raise SpiderVenomApiError(
                f"Spider Grills API returned HTTP {status}",
                status=status,
            )

        try:
            return json.loads(body) if body else {}
        except json.JSONDecodeError as err:
            raise SpiderVenomApiError("Spider Grills API response was not JSON") from err


def _auth_status(reason_str: str) -> int | None:
    """Return 403 if an MQTT disconnect/connect reason indicates an auth failure."""
    lowered = reason_str.lower()
    if any(marker in lowered for marker in ("not authorized", "bad user name or password")):
        return 403
    return None


def _log_response_body(
    label: str,
    status: int,
    body: str,
    *,
    redact_non_json: bool = False,
) -> None:
    """Log response bodies for local API discovery without leaking auth secrets."""
    if not _LOGGER.isEnabledFor(logging.DEBUG):
        return

    _LOGGER.debug(
        "%s returned HTTP %s body: %s",
        label,
        status,
        _redacted_body(body, redact_non_json=redact_non_json),
    )


def _redacted_body(body: str, *, redact_non_json: bool = False) -> str:
    if not body:
        return ""

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        if redact_non_json:
            return "<non-JSON response body omitted>"
        return _truncate(body, _MAX_NON_JSON_LOG_CHARS)

    return _truncate(json.dumps(_redact_value(parsed), sort_keys=True), _MAX_DEBUG_LOG_CHARS)


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return f"{text[:limit]}...<truncated>"


def _redact_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: "***REDACTED***" if _is_sensitive_key(key) else _redact_value(child)
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [_redact_value(child) for child in value]
    return value


def _is_sensitive_key(key: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", key.lower())
    return normalized in _REDACTED_KEYS or normalized.endswith("token")
