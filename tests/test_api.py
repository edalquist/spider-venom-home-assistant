"""Tests for Spider Grills Venom API client."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
import hashlib
import hmac
import json
import logging
import ssl
from typing import Any
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, quote, urlparse
import uuid

from aiohttp import ClientConnectorError, ClientTimeout
import paho.mqtt.client as mqtt
import pytest

from custom_components.spider_venom.api import (
    DEFAULT_TIMEOUT,
    SpiderGrillsAccountClient,
    SpiderVenomApiError,
    SpiderVenomAuthError,
    SpiderVenomClient,
    _Credentials,
    _redacted_body,
    _safe_call_soon_threadsafe,
)


class MockResponse:
    """Minimal async HTTP response mock."""

    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self._body = body

    async def text(self) -> str:
        return self._body

    async def __aenter__(self) -> MockResponse:
        return self

    async def __aexit__(self, *args: Any) -> None:
        return None


class FakeReasonCode:
    """Minimal paho ReasonCode stub."""

    def __init__(self, is_failure: bool = False, text: str = "Success") -> None:
        self.is_failure = is_failure
        self._text = text

    def __str__(self) -> str:
        return self._text


class FakeMQTTMessage:
    """Minimal paho MQTTMessage stub."""

    def __init__(self, topic: str, payload: bytes) -> None:
        self.topic = topic
        self.payload = payload


def _make_client(session: MagicMock | None = None, thing_name: str = "venom-123") -> SpiderVenomClient:
    return SpiderVenomClient(
        session=session or MagicMock(),
        endpoint="https://example-ats.iot.us-east-2.amazonaws.com/",
        identity_pool_id="us-east-2:11111111-2222-3333-4444-555555555555",
        region="us-east-2",
        thing_name=thing_name,
    )


def _valid_creds() -> _Credentials:
    return _Credentials(
        access_key_id="AKIA1",
        secret_key="SEC1",
        session_token="TOK1/+=special",
        expiration=datetime.now(UTC) + timedelta(hours=1),
        identity_id="us-east-2:id-1",
    )


def test_presigned_ws_path_sigv4_and_security_token_post_signing() -> None:
    """Verify SigV4 presigned WebSocket URL signs without security token and appends it after."""
    client = _make_client()
    creds = _valid_creds()
    ws_path = client._presigned_ws_path(creds)

    parsed = urlparse(ws_path)
    assert parsed.path == "/mqtt"
    params = parse_qs(parsed.query)
    assert params["X-Amz-Algorithm"] == ["AWS4-HMAC-SHA256"]
    assert params["X-Amz-SignedHeaders"] == ["host"]
    assert params["X-Amz-Security-Token"] == [creds.session_token]

    amz_date = params["X-Amz-Date"][0]
    date_stamp = amz_date[:8]
    scope = f"{date_stamp}/us-east-2/iotdevicegateway/aws4_request"
    assert params["X-Amz-Credential"] == [f"AKIA1/{scope}"]

    signed_query = "&".join(
        f"{k}={quote(v[0], safe='')}"
        for k, v in sorted(params.items())
        if k not in ("X-Amz-Signature", "X-Amz-Security-Token")
    )
    canonical_request = "\n".join(
        [
            "GET",
            "/mqtt",
            signed_query,
            "host:example-ats.iot.us-east-2.amazonaws.com\n",
            "host",
            hashlib.sha256(b"").hexdigest(),
        ]
    )
    sts = f"AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"
    expected_sig = hmac.new(
        client._signing_key(creds.secret_key, date_stamp, "iotdevicegateway"),
        sts.encode(),
        hashlib.sha256,
    ).hexdigest()
    assert params["X-Amz-Signature"] == [expected_sig]
    assert ws_path.endswith(f"&X-Amz-Security-Token={quote(creds.session_token, safe='')}")


async def test_mqtt_shadow_get_and_update_filters_client_token_and_caches_ssl() -> None:
    """Verify dashed UUID client_id, cached SSLContext, clientToken filtering, and cleanup."""
    client = _make_client()
    client._credentials = _valid_creds()
    created_clients: list[Any] = []
    published_messages: list[tuple[str, str]] = []

    class FakeClient:
        def __init__(
            self,
            api_version: Any,
            *,
            client_id: str,
            transport: str,
            reconnect_on_failure: bool = True,
        ) -> None:
            assert api_version is mqtt.CallbackAPIVersion.VERSION2
            assert transport == "websockets"
            assert reconnect_on_failure is False
            assert str(uuid.UUID(client_id)) == client_id
            self.connect_timeout: float = 0.0
            self.on_connect: Any = None
            self.on_subscribe: Any = None
            self.on_message: Any = None
            self.on_disconnect: Any = None
            self.disconnected = False
            self.loop_stopped = False
            created_clients.append(self)

        def ws_set_options(self, *, path: str) -> None:
            assert path.startswith("/mqtt?")

        def tls_set_context(self, ctx: ssl.SSLContext) -> None:
            pass

        def connect(self, host: str, port: int, keepalive: int = 60) -> None:
            assert host == "example-ats.iot.us-east-2.amazonaws.com"
            assert port == 443
            assert self.connect_timeout == 10.0

        def subscribe(self, topics: list[tuple[str, int]]) -> tuple[int, int]:
            assert topics[0][0].endswith("/accepted")
            assert topics[1][0].endswith("/rejected")
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)])
            return (mqtt.MQTT_ERR_SUCCESS, 1)

        def publish(self, topic: str, payload: str, qos: int = 1) -> Any:
            published_messages.append((topic, payload))
            req = json.loads(payload)
            self.on_message(
                self,
                None,
                FakeMQTTMessage(
                    f"{topic}/accepted",
                    json.dumps({"clientToken": "other-app-token", "state": {"reported": {"mainTemp": 999}}}).encode(),
                ),
            )
            self.on_message(
                self,
                None,
                FakeMQTTMessage(
                    f"{topic}/accepted",
                    json.dumps({"clientToken": req["clientToken"], "state": {"reported": {"mainTemp": 225}}}).encode(),
                ),
            )
            return MagicMock(rc=mqtt.MQTT_ERR_SUCCESS)

        def loop_start(self) -> None:
            self.on_connect(self, None, None, FakeReasonCode(False))

        def disconnect(self) -> None:
            self.disconnected = True

        def loop_stop(self) -> None:
            self.loop_stopped = True

    fake_ssl_ctx = MagicMock(spec=ssl.SSLContext)
    with (
        patch("custom_components.spider_venom.api.mqtt.Client", FakeClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=fake_ssl_ctx) as mock_ssl,
    ):
        shadow = await client.async_get_shadow()
        assert shadow["state"]["reported"]["mainTemp"] == 225

        await client.async_set_target_temperature(300)

    mock_ssl.assert_called_once()
    assert len(created_clients) == 2
    assert all(c.disconnected and c.loop_stopped for c in created_clients)
    assert published_messages[0][0] == "$aws/things/venom-123/shadow/get"
    assert published_messages[1][0] == "$aws/things/venom-123/shadow/update"
    update_payload = json.loads(published_messages[1][1])
    assert update_payload["state"] == {"desired": {"heat": {"t2": {"trgt": 300}}}}


async def test_mqtt_rejected_logs_actual_code_and_raises(caplog: pytest.LogCaptureFixture) -> None:
    """Verify /rejected shadow messages log the rejection code and raise SpiderVenomApiError."""
    client = _make_client()
    client._credentials = _valid_creds()

    class RejectedClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.on_connect: Any = None
            self.on_subscribe: Any = None
            self.on_message: Any = None
            self.on_disconnect: Any = None

        def ws_set_options(self, **kwargs: Any) -> None:
            pass

        def tls_set_context(self, ctx: Any) -> None:
            pass

        def connect(self, *args: Any, **kwargs: Any) -> None:
            pass

        def subscribe(self, topics: Any) -> tuple[int, int]:
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)])
            return (mqtt.MQTT_ERR_SUCCESS, 1)

        def publish(self, topic: str, payload: str, qos: int = 1) -> Any:
            token = json.loads(payload)["clientToken"]
            self.on_message(
                self,
                None,
                FakeMQTTMessage(
                    f"{topic}/rejected",
                    json.dumps({"clientToken": token, "code": 400, "message": "Invalid payload"}).encode(),
                ),
            )
            return MagicMock(rc=mqtt.MQTT_ERR_SUCCESS)

        def loop_start(self) -> None:
            self.on_connect(self, None, None, FakeReasonCode(False))

        def disconnect(self) -> None:
            pass

        def loop_stop(self) -> None:
            pass

    with (
        caplog.at_level(logging.DEBUG, logger="custom_components.spider_venom.api"),
        patch("custom_components.spider_venom.api.mqtt.Client", RejectedClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
        pytest.raises(SpiderVenomApiError, match="Shadow get rejected: 400 Invalid payload") as exc_info,
    ):
        await client.async_get_shadow()

    assert exc_info.value.status == 400
    assert "returned HTTP 400 body:" in caplog.text


async def test_mqtt_auth_failure_invalidates_credentials_and_retries_once() -> None:
    """Verify 403 WebSocket error refreshes credentials and retries once."""
    session = MagicMock()
    future_exp = (datetime.now(UTC) + timedelta(hours=1)).timestamp()
    session.post.side_effect = [
        MockResponse(
            200,
            json.dumps(
                {
                    "Credentials": {
                        "AccessKeyId": "AK_NEW",
                        "SecretKey": "SK_NEW",
                        "SessionToken": "ST_NEW",
                        "Expiration": future_exp,
                    }
                }
            ),
        )
    ]

    client = _make_client(session)
    client._identity_id = "us-east-2:id-1"
    client._credentials = _valid_creds()
    attempts = 0

    class AuthRetryClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.on_connect: Any = None
            self.on_subscribe: Any = None
            self.on_message: Any = None
            self.on_disconnect: Any = None

        def ws_set_options(self, **kwargs: Any) -> None:
            pass

        def tls_set_context(self, ctx: Any) -> None:
            pass

        def connect(self, *args: Any, **kwargs: Any) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise mqtt.WebsocketConnectionError("WebSocket handshake error, connection not upgraded")

        def subscribe(self, topics: Any) -> tuple[int, int]:
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)])
            return (mqtt.MQTT_ERR_SUCCESS, 1)

        def publish(self, topic: str, payload: str, qos: int = 1) -> Any:
            token = json.loads(payload)["clientToken"]
            self.on_message(
                self,
                None,
                FakeMQTTMessage(
                    f"{topic}/accepted",
                    json.dumps({"clientToken": token, "state": {"reported": {"mainTemp": 230}}}).encode(),
                ),
            )
            return MagicMock(rc=mqtt.MQTT_ERR_SUCCESS)

        def loop_start(self) -> None:
            self.on_connect(self, None, None, FakeReasonCode(False))

        def disconnect(self) -> None:
            pass

        def loop_stop(self) -> None:
            pass

    with (
        patch("custom_components.spider_venom.api.mqtt.Client", AuthRetryClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
    ):
        result = await client.async_get_shadow()

    assert attempts == 2
    assert result["state"]["reported"]["mainTemp"] == 230
    assert client._credentials.access_key_id == "AK_NEW"


async def test_mqtt_timeout_raises_api_error() -> None:
    """Verify MQTT exchange timeout raises SpiderVenomApiError."""
    client = _make_client()
    client._credentials = _valid_creds()

    class HangingClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.on_connect: Any = None
            self.on_subscribe: Any = None
            self.on_message: Any = None
            self.on_disconnect: Any = None

        def ws_set_options(self, **kwargs: Any) -> None:
            pass

        def tls_set_context(self, ctx: Any) -> None:
            pass

        def connect(self, *args: Any, **kwargs: Any) -> None:
            pass

        def loop_start(self) -> None:
            pass

        def disconnect(self) -> None:
            pass

        def loop_stop(self) -> None:
            pass

    with (
        patch("custom_components.spider_venom.api._MQTT_TIMEOUT", 0.01),
        patch("custom_components.spider_venom.api.mqtt.Client", HangingClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
        pytest.raises(SpiderVenomApiError, match="timed out"),
    ):
        await client.async_get_shadow()


async def test_cognito_locking_identity_reuse_and_errors() -> None:
    """Verify Cognito locking, identity_id reuse, HTTP status-before-JSON, and network errors."""
    session = MagicMock()
    future_exp = (datetime.now(UTC) + timedelta(hours=1)).timestamp()
    post_calls: list[str] = []

    def fake_post(url: str, *, headers: dict[str, str], json: dict[str, Any], timeout: ClientTimeout) -> MockResponse:
        target = headers["X-Amz-Target"].split(".")[-1]
        post_calls.append(target)
        assert timeout == DEFAULT_TIMEOUT
        if target == "GetId":
            return MockResponse(200, '{"IdentityId":"us-east-2:id-1"}')
        return MockResponse(
            200,
            f'{{"Credentials":{{"AccessKeyId":"AK1","SecretKey":"SK1","SessionToken":"ST1","Expiration":{future_exp}}}}}',
        )

    session.post.side_effect = fake_post
    client = _make_client(session)
    results = await asyncio.gather(*(client._async_get_credentials() for _ in range(5)))
    assert all(r.identity_id == "us-east-2:id-1" for r in results)
    assert post_calls == ["GetId", "GetCredentialsForIdentity"]

    client._credentials = _Credentials(
        access_key_id="AK1",
        secret_key="SK1",
        session_token="ST1",
        expiration=datetime.now(UTC) + timedelta(minutes=1),
        identity_id="us-east-2:id-1",
    )
    await client._async_get_credentials()
    assert post_calls == ["GetId", "GetCredentialsForIdentity", "GetCredentialsForIdentity"]

    session.post.side_effect = None
    session.post.return_value = MockResponse(503, "Service Unavailable")
    with pytest.raises(SpiderVenomApiError, match="HTTP 503"):
        await client._async_cognito_post("GetId", {"IdentityPoolId": "pool"})

    session.post.side_effect = ClientConnectorError(MagicMock(), OSError("refused"))
    with pytest.raises(SpiderVenomApiError, match="could not connect"):
        await client._async_cognito_post("GetId", {"IdentityPoolId": "pool"})


async def test_account_client_login_devices_and_http_checks() -> None:
    """Verify SpiderGrillsAccountClient login, auth error, shape guards, and status checks."""
    session = MagicMock()
    account = SpiderGrillsAccountClient(session=session)

    session.request.return_value = MockResponse(200, json.dumps({"accessToken": "tok-123"}))
    assert await account.async_login(email="user@example.com", password="pw") == "tok-123"
    assert session.request.call_args.kwargs["timeout"] == DEFAULT_TIMEOUT

    session.request.return_value = MockResponse(200, "[]")
    with pytest.raises(SpiderVenomApiError, match="unexpected shape"):
        await account.async_login(email="user@example.com", password="pw")

    session.request.return_value = MockResponse(401, "Unauthorized")
    with pytest.raises(SpiderVenomAuthError, match="HTTP 401"):
        await account.async_login(email="user@example.com", password="pw")

    session.request.return_value = MockResponse(502, "<html>Bad Gateway</html>")
    with pytest.raises(SpiderVenomApiError, match="HTTP 502"):
        await account.async_login(email="user@example.com", password="pw")

    for payload in (
        [{"thingName": "v1"}],
        {"data": [{"thingName": "v1"}]},
        {"devices": [{"thingName": "v1"}]},
    ):
        session.request.return_value = MockResponse(200, json.dumps(payload))
        assert await account.async_get_devices("tok-123") == [{"thingName": "v1"}]

    session.request.return_value = MockResponse(200, "null")
    with pytest.raises(SpiderVenomApiError, match="unexpected shape"):
        await account.async_get_devices("tok-123")


def test_redacted_body_masks_sensitive_fields_and_truncates() -> None:
    """Verify normalized key redaction, truncation, and login non-JSON omission."""
    raw = json.dumps(
        {
            "accessToken": "secret-access",
            "refreshToken": "secret-refresh",
            "password": "secret-password",
            "email": "user@example.com",
            "identityId": "us-east-2:123",
            "Credentials": {
                "AccessKeyId": "AKIASECRET",
                "secret_access_key": "SECRETKEY",
                "SessionToken": "SESSIONTOK",
            },
            "reported": {
                "ssid": "HomeNetwork",
                "bssid": "11:22:33:44:55:66",
                "mac": "AA:BB:CC:DD:EE:FF",
            },
            "profile": {
                "firstName": "Jane",
                "lastName": "Doe",
                "phone": "555-0100",
                "address": "123 Patio Ln",
            },
            "device": {"thingName": "venom-1"},
        }
    )
    redacted = json.loads(_redacted_body(raw))

    assert redacted["accessToken"] == "***REDACTED***"
    assert redacted["refreshToken"] == "***REDACTED***"
    assert redacted["password"] == "***REDACTED***"
    assert redacted["email"] == "***REDACTED***"
    assert redacted["identityId"] == "***REDACTED***"
    assert redacted["Credentials"]["AccessKeyId"] == "***REDACTED***"
    assert redacted["Credentials"]["secret_access_key"] == "***REDACTED***"
    assert redacted["Credentials"]["SessionToken"] == "***REDACTED***"
    assert redacted["reported"]["ssid"] == "***REDACTED***"
    assert redacted["reported"]["bssid"] == "***REDACTED***"
    assert redacted["reported"]["mac"] == "***REDACTED***"
    assert redacted["profile"]["firstName"] == "***REDACTED***"
    assert redacted["profile"]["lastName"] == "***REDACTED***"
    assert redacted["profile"]["phone"] == "***REDACTED***"
    assert redacted["profile"]["address"] == "***REDACTED***"
    assert redacted["device"]["thingName"] == "venom-1"

    assert _redacted_body("<html>err</html>", redact_non_json=True) == "<non-JSON response body omitted>"
    assert _redacted_body(json.dumps({"data": "x" * 5000})).endswith("...<truncated>")


async def test_cognito_error_body_is_redacted_in_exception() -> None:
    """Verify non-200 Cognito responses without __type redact sensitive JSON fields."""
    session = MagicMock()
    session.post.return_value = MockResponse(
        400,
        json.dumps({"email": "secret@example.com", "message": "Bad identity"}),
    )
    client = _make_client(session)
    with pytest.raises(SpiderVenomApiError) as exc_info:
        await client._async_cognito_post("GetId", {"IdentityPoolId": "pool"})
    assert "secret@example.com" not in str(exc_info.value)
    assert "***REDACTED***" in str(exc_info.value)


@pytest.mark.no_mock_mqtt_push
async def test_persistent_mqtt_push_subscriptions_requests_and_stop() -> None:
    """Verify persistent MQTT mode subscribes to 5 topics, reuses socket, delivers pushes, and stops."""
    client = _make_client()
    client._credentials = _valid_creds()
    created_clients: list[Any] = []
    subscribed_topics: list[list[str]] = []
    pushed_docs: list[dict[str, Any]] = []

    class PersistentFakeClient:
        def __init__(
            self,
            api_version: Any,
            *,
            client_id: str,
            transport: str,
            reconnect_on_failure: bool = True,
        ) -> None:
            assert reconnect_on_failure is False
            self.on_connect: Any = None
            self.on_subscribe: Any = None
            self.on_message: Any = None
            self.on_disconnect: Any = None
            self.connect_timeout: float = 0.0
            self.disconnected = False
            self.loop_stopped = False
            created_clients.append(self)

        def ws_set_options(self, *, path: str) -> None:
            pass

        def tls_set_context(self, ctx: ssl.SSLContext) -> None:
            pass

        def connect(self, host: str, port: int, keepalive: int = 60) -> None:
            pass

        def subscribe(self, topics: list[tuple[str, int]]) -> tuple[int, int]:
            subscribed_topics.append([t for t, _qos in topics])
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)] * len(topics))
            return (mqtt.MQTT_ERR_SUCCESS, 1)

        def publish(self, topic: str, payload: str, qos: int = 1) -> Any:
            req = json.loads(payload)
            self.on_message(
                self,
                None,
                FakeMQTTMessage(
                    f"{topic}/accepted",
                    json.dumps({"clientToken": req["clientToken"], "state": {"reported": {"mainTemp": 260}}}).encode(),
                ),
            )
            return MagicMock(rc=mqtt.MQTT_ERR_SUCCESS)

        def loop_start(self) -> None:
            self.on_connect(self, None, None, FakeReasonCode(False))

        def disconnect(self) -> None:
            self.disconnected = True

        def loop_stop(self) -> None:
            self.loop_stopped = True

    with (
        patch("custom_components.spider_venom.api.mqtt.Client", PersistentFakeClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
    ):
        await client.async_start(pushed_docs.append)
        assert len(created_clients) == 1
        assert subscribed_topics == [
            [
                "$aws/things/venom-123/shadow/get/accepted",
                "$aws/things/venom-123/shadow/get/rejected",
                "$aws/things/venom-123/shadow/update/accepted",
                "$aws/things/venom-123/shadow/update/rejected",
                "$aws/things/venom-123/shadow/update/documents",
            ]
        ]

        # Reuses existing connection for get & update
        got = await client.async_get_shadow()
        assert got["state"]["reported"]["mainTemp"] == 260
        await client.async_set_target_temperature(300)
        assert len(created_clients) == 1

        # Push update on shadow/update/documents delivers current + timestamp
        created_clients[0].on_message(
            created_clients[0],
            None,
            FakeMQTTMessage(
                "$aws/things/venom-123/shadow/update/documents",
                json.dumps({"current": {"state": {"reported": {"mainTemp": 275}}}, "timestamp": 1700000999}).encode(),
            ),
        )
        await asyncio.sleep(0)
        assert pushed_docs == [{"state": {"reported": {"mainTemp": 275}}, "timestamp": 1700000999}]

        await client.async_stop()
        assert client._runner_task is None
        assert created_clients[0].disconnected
        assert created_clients[0].loop_stopped


@pytest.mark.no_mock_mqtt_push
async def test_persistent_mqtt_rotates_credentials_and_reconnects_on_disconnect() -> None:
    """Verify proactive rotation before credential expiry and reconnect after unexpected disconnect."""
    session = MagicMock()
    soon_exp = (datetime.now(UTC) + timedelta(minutes=5, seconds=0.05)).timestamp()
    later_exp = (datetime.now(UTC) + timedelta(hours=1)).timestamp()
    session.post.side_effect = [
        MockResponse(
            200,
            json.dumps(
                {
                    "Credentials": {
                        "AccessKeyId": "AK2",
                        "SecretKey": "SK2",
                        "SessionToken": "ST2",
                        "Expiration": later_exp,
                    }
                }
            ),
        ),
        MockResponse(
            200,
            json.dumps(
                {
                    "Credentials": {
                        "AccessKeyId": "AK3",
                        "SecretKey": "SK3",
                        "SessionToken": "ST3",
                        "Expiration": later_exp,
                    }
                }
            ),
        ),
    ]

    client = _make_client(session)
    client._identity_id = "us-east-2:id-1"
    client._credentials = _Credentials(
        access_key_id="AK1",
        secret_key="SK1",
        session_token="ST1",
        expiration=datetime.fromtimestamp(soon_exp, UTC),
        identity_id="us-east-2:id-1",
    )
    created_clients: list[Any] = []
    reconnect_event = asyncio.Event()

    class ReconnectFakeClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.on_connect: Any = None
            self.on_subscribe: Any = None
            self.on_message: Any = None
            self.on_disconnect: Any = None
            created_clients.append(self)
            if len(created_clients) >= 3:
                reconnect_event.set()

        def ws_set_options(self, **kwargs: Any) -> None:
            pass

        def tls_set_context(self, ctx: Any) -> None:
            pass

        def connect(self, *args: Any, **kwargs: Any) -> None:
            pass

        def subscribe(self, topics: list[tuple[str, int]]) -> tuple[int, int]:
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)] * len(topics))
            return (mqtt.MQTT_ERR_SUCCESS, 1)

        def loop_start(self) -> None:
            self.on_connect(self, None, None, FakeReasonCode(False))

        def disconnect(self) -> None:
            pass

        def loop_stop(self) -> None:
            pass

    with (
        patch("custom_components.spider_venom.api.mqtt.Client", ReconnectFakeClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
        patch.object(client, "_async_sleep_interruptible", return_value=None),
    ):
        await client.async_start(lambda _doc: None)
        # Wait for proactive rotation (connect #2)
        for _ in range(30):
            if len(created_clients) >= 2:
                break
            await asyncio.sleep(0.01)
        assert len(created_clients) >= 2
        assert client._credentials is not None
        assert client._credentials.access_key_id == "AK2"

        # Simulate unexpected MQTT v3.1.1 server disconnect -> clears credentials and reconnects (#3)
        created_clients[1].on_disconnect(
            created_clients[1], None, None, FakeReasonCode(True, "Unspecified error")
        )
        await asyncio.wait_for(reconnect_event.wait(), timeout=2.0)
        assert len(created_clients) >= 3
        assert client._credentials is not None
        assert client._credentials.access_key_id == "AK3"

        await client.async_stop()


@pytest.mark.no_mock_mqtt_push
async def test_persistent_mqtt_initial_start_failures_and_auth_retry() -> None:
    """Verify persistent async_start handles timeouts, connect/subscribe refusals, and auth retry."""
    client = _make_client()
    client._credentials = _valid_creds()

    class TimeoutClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.on_connect: Any = None
            self.on_subscribe: Any = None
            self.on_message: Any = None
            self.on_disconnect: Any = None

        def ws_set_options(self, **kwargs: Any) -> None:
            pass

        def tls_set_context(self, ctx: Any) -> None:
            pass

        def connect(self, *args: Any, **kwargs: Any) -> None:
            pass

        def loop_start(self) -> None:
            pass

        def disconnect(self) -> None:
            pass

        def loop_stop(self) -> None:
            pass

    with (
        patch("custom_components.spider_venom.api._MQTT_TIMEOUT", 0.05),
        patch("custom_components.spider_venom.api.mqtt.Client", TimeoutClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
        pytest.raises(SpiderVenomApiError, match="timed out"),
    ):
        await client.async_start(lambda _doc: None)

    class ConnectRefusedClient(TimeoutClient):
        def loop_start(self) -> None:
            self.on_connect(self, None, None, FakeReasonCode(True, "Server unavailable"))

    with (
        patch("custom_components.spider_venom.api.mqtt.Client", ConnectRefusedClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
        pytest.raises(SpiderVenomApiError, match="MQTT connect refused"),
    ):
        await client.async_start(lambda _doc: None)

    class SubscribeRefusedClient(TimeoutClient):
        def subscribe(self, topics: Any) -> tuple[int, int]:
            self.on_subscribe(self, None, 1, [FakeReasonCode(True, "Unspecified error")])
            return (mqtt.MQTT_ERR_SUCCESS, 1)

        def loop_start(self) -> None:
            self.on_connect(self, None, None, FakeReasonCode(False))

    with (
        patch("custom_components.spider_venom.api.mqtt.Client", SubscribeRefusedClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
        pytest.raises(SpiderVenomApiError, match="MQTT subscribe refused"),
    ):
        await client.async_start(lambda _doc: None)

    class HandshakeDisconnectClient(TimeoutClient):
        def loop_start(self) -> None:
            self.on_disconnect(self, None, None, FakeReasonCode(True, "Unspecified error"))

    with (
        patch("custom_components.spider_venom.api.mqtt.Client", HandshakeDisconnectClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
        pytest.raises(SpiderVenomApiError, match="MQTT disconnected"),
    ):
        await client.async_start(lambda _doc: None)

    # Initial auth failure (Not authorized CONNACK) invalidates credentials and retries once
    session = MagicMock()
    future_exp = (datetime.now(UTC) + timedelta(hours=1)).timestamp()
    session.post.return_value = MockResponse(
        200,
        json.dumps(
            {
                "Credentials": {
                    "AccessKeyId": "AK_RETRY",
                    "SecretKey": "SK_RETRY",
                    "SessionToken": "ST_RETRY",
                    "Expiration": future_exp,
                }
            }
        ),
    )
    auth_client = _make_client(session)
    auth_client._identity_id = "us-east-2:id-1"
    auth_client._credentials = _valid_creds()
    attempts = 0

    class InitialAuthRetryClient(TimeoutClient):
        def subscribe(self, topics: list[tuple[str, int]]) -> tuple[int, int]:
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)] * len(topics))
            return (mqtt.MQTT_ERR_SUCCESS, 1)

        def loop_start(self) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                self.on_connect(self, None, None, FakeReasonCode(True, "Not authorized"))
            else:
                self.on_connect(self, None, None, FakeReasonCode(False))

    with (
        patch("custom_components.spider_venom.api.mqtt.Client", InitialAuthRetryClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
    ):
        await auth_client.async_start(lambda _doc: None)
        assert attempts == 2
        assert auth_client._credentials is not None
        assert auth_client._credentials.access_key_id == "AK_RETRY"
        await auth_client.async_stop()

    # Persistent initial WebsocketConnectionError also invalidates credentials and retries once
    ws_session = MagicMock()
    ws_session.post.return_value = MockResponse(
        200,
        json.dumps(
            {
                "Credentials": {
                    "AccessKeyId": "AK_WS_RETRY",
                    "SecretKey": "SK_WS_RETRY",
                    "SessionToken": "ST_WS_RETRY",
                    "Expiration": future_exp,
                }
            }
        ),
    )
    ws_client = _make_client(ws_session)
    ws_client._identity_id = "us-east-2:id-1"
    ws_client._credentials = _valid_creds()
    ws_attempts = 0

    class InitialWsAuthRetryClient(TimeoutClient):
        def connect(self, *args: Any, **kwargs: Any) -> None:
            nonlocal ws_attempts
            ws_attempts += 1
            if ws_attempts == 1:
                raise mqtt.WebsocketConnectionError("WebSocket handshake error, connection not upgraded")

        def subscribe(self, topics: list[tuple[str, int]]) -> tuple[int, int]:
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)] * len(topics))
            return (mqtt.MQTT_ERR_SUCCESS, 1)

        def loop_start(self) -> None:
            self.on_connect(self, None, None, FakeReasonCode(False))

    with (
        patch("custom_components.spider_venom.api.mqtt.Client", InitialWsAuthRetryClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
    ):
        await ws_client.async_start(lambda _doc: None)
        assert ws_attempts == 2
        assert ws_client._credentials is not None
        assert ws_client._credentials.access_key_id == "AK_WS_RETRY"
        await ws_client.async_stop()


@pytest.mark.no_mock_mqtt_push
async def test_persistent_mqtt_request_errors_malformed_messages_and_disconnect() -> None:
    """Verify persistent request timeouts, publish failures, rejections, malformed payloads, and fail_pending."""
    client = _make_client()
    client._credentials = _valid_creds()
    pushed_docs: list[dict[str, Any]] = []
    publish_mode = "ok"
    created: list[Any] = []

    class PersistentErrorClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.on_connect: Any = None
            self.on_subscribe: Any = None
            self.on_message: Any = None
            self.on_disconnect: Any = None
            created.append(self)

        def ws_set_options(self, **kwargs: Any) -> None:
            pass

        def tls_set_context(self, ctx: Any) -> None:
            pass

        def connect(self, *args: Any, **kwargs: Any) -> None:
            pass

        def subscribe(self, topics: list[tuple[str, int]]) -> tuple[int, int]:
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)] * len(topics))
            return (mqtt.MQTT_ERR_SUCCESS, 1)

        def publish(self, topic: str, payload: str, qos: int = 1) -> Any:
            if publish_mode == "rc_error":
                return MagicMock(rc=mqtt.MQTT_ERR_NO_CONN)
            if publish_mode == "rejected":
                token = json.loads(payload)["clientToken"]
                self.on_message(
                    self,
                    None,
                    FakeMQTTMessage(
                        f"{topic}/rejected",
                        json.dumps({"clientToken": token, "code": 409, "message": "Version conflict"}).encode(),
                    ),
                )
            elif publish_mode == "disconnect":
                self.on_disconnect(self, None, None, FakeReasonCode(True, "Unspecified error"))
            return MagicMock(rc=mqtt.MQTT_ERR_SUCCESS)

        def loop_start(self) -> None:
            self.on_connect(self, None, None, FakeReasonCode(False))

        def disconnect(self) -> None:
            pass

        def loop_stop(self) -> None:
            pass

    with (
        patch("custom_components.spider_venom.api.mqtt.Client", PersistentErrorClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
        patch.object(client, "_async_get_credentials", return_value=_valid_creds()),
        patch.object(client, "_async_sleep_interruptible", return_value=None),
    ):
        await client.async_start(pushed_docs.append)
        mqtt_c = created[0]

        # Malformed/non-dict payloads and missing clientToken are safely ignored
        for topic, raw_bytes in (
            ("$aws/things/venom-123/shadow/update/documents", b"{bad-json"),
            ("$aws/things/venom-123/shadow/update/documents", b"[]"),
            ("$aws/things/venom-123/shadow/update/documents", b'{"current": "not-a-dict"}'),
            ("$aws/things/venom-123/shadow/get/accepted", b'{"state": {}}'),
        ):
            mqtt_c.on_message(mqtt_c, None, FakeMQTTMessage(topic, raw_bytes))
        await asyncio.sleep(0)
        assert pushed_docs == []

        # Non-zero publish return code
        publish_mode = "rc_error"
        with pytest.raises(SpiderVenomApiError, match="publish failed"):
            await client.async_get_shadow()

        # /rejected shadow response
        publish_mode = "rejected"
        with pytest.raises(SpiderVenomApiError, match="Shadow update rejected: 409 Version conflict") as rej_exc:
            await client.async_set_target_temperature(280)
        assert rej_exc.value.status == 409

        # Timeout on unanswered persistent request
        publish_mode = "timeout"
        with (
            patch("custom_components.spider_venom.api._MQTT_TIMEOUT", 0.02),
            pytest.raises(SpiderVenomApiError, match="timed out"),
        ):
            await client.async_get_shadow()

        # Mid-flight disconnect triggers _fail_pending
        publish_mode = "disconnect"
        with pytest.raises(SpiderVenomApiError, match="connection closed"):
            await client.async_get_shadow()

        await client.async_stop()


async def test_cognito_edge_cases_and_identity_recovery() -> None:
    """Verify Cognito clears stale identity_id on 400/401/403 and validates response shapes."""
    session = MagicMock()
    future_exp = (datetime.now(UTC) + timedelta(hours=1)).timestamp()
    post_calls: list[str] = []
    cred_attempts = 0

    def fake_post(url: str, *, headers: dict[str, str], json: dict[str, Any], timeout: ClientTimeout) -> MockResponse:
        nonlocal cred_attempts
        target = headers["X-Amz-Target"].split(".")[-1]
        post_calls.append(target)
        if target == "GetId":
            return MockResponse(200, f'{{"IdentityId":"us-east-2:fresh-{len(post_calls)}"}}')
        cred_attempts += 1
        if cred_attempts == 1:
            return MockResponse(400, '{"__type":"ResourceNotFoundException","message":"Identity not found"}')
        return MockResponse(
            200,
            f'{{"Credentials":{{"AccessKeyId":"AK_F","SecretKey":"SK_F","SessionToken":"ST_F","Expiration":{future_exp}}}}}',
        )

    session.post.side_effect = fake_post
    client = _make_client(session)
    client._identity_id = "us-east-2:stale-id"

    with pytest.raises(SpiderVenomApiError, match="ResourceNotFoundException"):
        await client._async_get_credentials()
    assert client._identity_id is None

    creds = await client._async_get_credentials()
    assert creds.access_key_id == "AK_F"
    assert post_calls == ["GetCredentialsForIdentity", "GetId", "GetCredentialsForIdentity"]

    # Missing IdentityId / Credentials / incomplete fields / non-JSON 200
    session.post.side_effect = None
    client._identity_id = None
    client._credentials = None
    session.post.return_value = MockResponse(200, "{}")
    with pytest.raises(SpiderVenomApiError, match="did not include IdentityId"):
        await client._async_get_credentials()

    client._identity_id = "us-east-2:ok"
    session.post.return_value = MockResponse(200, "{}")
    with pytest.raises(SpiderVenomApiError, match="did not include credentials"):
        await client._async_get_credentials()

    session.post.return_value = MockResponse(200, '{"Credentials":{"AccessKeyId":"A"}}')
    with pytest.raises(SpiderVenomApiError, match="credentials response was incomplete"):
        await client._async_get_credentials()

    for bad_creds in (
        {"AccessKeyId": None, "SecretKey": "S", "SessionToken": "T", "Expiration": future_exp},
        {"AccessKeyId": "", "SecretKey": "S", "SessionToken": "T", "Expiration": future_exp},
    ):
        session.post.return_value = MockResponse(200, json.dumps({"Credentials": bad_creds}))
        with pytest.raises(SpiderVenomApiError, match="credentials response was incomplete"):
            await client._async_get_credentials()

    session.post.return_value = MockResponse(200, "not-json")
    with pytest.raises(SpiderVenomApiError, match="was not JSON"):
        await client._async_cognito_post("GetId", {"IdentityPoolId": "pool"})


async def test_account_client_error_cases() -> None:
    """Verify SpiderGrillsAccountClient handles 403, missing token, network errors, and non-JSON."""
    session = MagicMock()
    account = SpiderGrillsAccountClient(session=session)

    session.request.return_value = MockResponse(200, json.dumps({"accessToken": ""}))
    with pytest.raises(SpiderVenomApiError, match="did not include an access token"):
        await account.async_login(email="user@example.com", password="pw")

    session.request.return_value = MockResponse(403, "Forbidden")
    with pytest.raises(SpiderVenomAuthError, match="HTTP 403"):
        await account.async_login(email="user@example.com", password="pw")

    session.request.return_value = MockResponse(200, "not-json")
    with pytest.raises(SpiderVenomApiError, match="was not JSON"):
        await account.async_login(email="user@example.com", password="pw")

    session.request.side_effect = TimeoutError("timed out")
    with pytest.raises(SpiderVenomApiError, match="timed out"):
        await account.async_login(email="user@example.com", password="pw")

    session.request.side_effect = ClientConnectorError(MagicMock(), OSError("refused"))
    with pytest.raises(SpiderVenomApiError, match="could not connect"):
        await account.async_get_devices("tok")


@pytest.mark.no_mock_mqtt_push
async def test_async_stop_wakes_pending_subscribe_and_guards_loop_start() -> None:
    """Verify async_stop() returns promptly during subscribe and skips loop_start if stopping."""
    client = _make_client()
    client._credentials = _valid_creds()
    loop_started = False
    connected_event = asyncio.Event()

    class SlowSubscribeClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.on_connect: Any = None
            self.on_subscribe: Any = None
            self.on_message: Any = None
            self.on_disconnect: Any = None
            self.disconnected = False
            self.loop_stopped = False

        def ws_set_options(self, **kwargs: Any) -> None:
            pass

        def tls_set_context(self, ctx: Any) -> None:
            pass

        def connect(self, *args: Any, **kwargs: Any) -> None:
            connected_event.set()

        def loop_start(self) -> None:
            nonlocal loop_started
            loop_started = True
            self.on_connect(self, None, None, FakeReasonCode(False))

        def subscribe(self, topics: Any) -> tuple[int, int]:
            # Deliberately never fire on_subscribe
            return (mqtt.MQTT_ERR_SUCCESS, 1)

        def disconnect(self) -> None:
            self.disconnected = True

        def loop_stop(self) -> None:
            self.loop_stopped = True

    with (
        patch("custom_components.spider_venom.api.mqtt.Client", SlowSubscribeClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
    ):
        start_task = asyncio.create_task(client.async_start(lambda _doc: None))
        await asyncio.wait_for(connected_event.wait(), timeout=1.0)
        await asyncio.sleep(0)
        # async_stop should wake the subscribe wait promptly (well below _MQTT_TIMEOUT)
        await asyncio.wait_for(client.async_stop(), timeout=1.0)
        await asyncio.wait_for(start_task, timeout=1.0)
        assert loop_started is True

    # Direct check: if _stopping becomes True during connect(), loop_start() is never called
    client._stopping = True
    mock_mqtt = MagicMock()
    client._connect_and_start_client(mock_mqtt, honor_stopping=True)
    mock_mqtt.connect.assert_called_once()
    mock_mqtt.disconnect.assert_called_once()
    mock_mqtt.loop_start.assert_not_called()


@pytest.mark.no_mock_mqtt_push
async def test_persistent_mqtt_unexpected_exception_fails_fast_and_recovers_on_reconnect(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify unexpected non-API exceptions fail fast on initial connect and recover after reconnect."""
    client = _make_client()
    client._credentials = _valid_creds()

    with (
        patch.object(client, "_presigned_ws_path", side_effect=RuntimeError("bad signer state")),
        pytest.raises(SpiderVenomApiError, match="Unexpected IoT MQTT error: bad signer state"),
    ):
        await asyncio.wait_for(client.async_start(lambda _doc: None), timeout=1.0)

    # Unexpected exception after initial connect logs and reconnects cleanly
    client2 = _make_client()
    client2._credentials = _valid_creds()
    ws_calls = 0
    recovered_event = asyncio.Event()
    original_presign = client2._presigned_ws_path

    def flaky_presign(creds: _Credentials) -> str:
        nonlocal ws_calls
        ws_calls += 1
        if ws_calls == 2:
            raise RuntimeError("transient signing glitch")
        return original_presign(creds)

    created: list[Any] = []

    class RecoverClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.on_connect: Any = None
            self.on_subscribe: Any = None
            self.on_message: Any = None
            self.on_disconnect: Any = None
            created.append(self)

        def ws_set_options(self, **kwargs: Any) -> None:
            pass

        def tls_set_context(self, ctx: Any) -> None:
            pass

        def connect(self, *args: Any, **kwargs: Any) -> None:
            pass

        def subscribe(self, topics: list[tuple[str, int]]) -> tuple[int, int]:
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)] * len(topics))
            if len(created) >= 2:
                recovered_event.set()
            return (mqtt.MQTT_ERR_SUCCESS, 1)

        def loop_start(self) -> None:
            self.on_connect(self, None, None, FakeReasonCode(False))

        def disconnect(self) -> None:
            pass

        def loop_stop(self) -> None:
            pass

    with (
        caplog.at_level(logging.ERROR, logger="custom_components.spider_venom.api"),
        patch.object(client2, "_presigned_ws_path", side_effect=flaky_presign),
        patch.object(client2, "_async_get_credentials", return_value=_valid_creds()),
        patch("custom_components.spider_venom.api.mqtt.Client", RecoverClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
        patch.object(client2, "_async_sleep_interruptible", return_value=None),
    ):
        await client2.async_start(lambda _doc: None)
        created[0].on_disconnect(created[0], None, None, FakeReasonCode(True, "Unspecified error"))
        await asyncio.wait_for(recovered_event.wait(), timeout=1.0)
        assert ws_calls >= 3
        assert "Unexpected error in Spider Venom MQTT loop" in caplog.text
        await client2.async_stop()


@pytest.mark.no_mock_mqtt_push
async def test_persistent_mqtt_clamps_rotation_delay_under_clock_skew(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify near-expiry Cognito credentials clamp rotation delay to _MIN_ROTATION_DELAY."""
    session = MagicMock()
    soon_exp = (datetime.now(UTC) + timedelta(minutes=2)).timestamp()
    later_exp = (datetime.now(UTC) + timedelta(hours=1)).timestamp()
    session.post.side_effect = [
        MockResponse(
            200,
            json.dumps(
                {
                    "Credentials": {
                        "AccessKeyId": "AK_SKEW",
                        "SecretKey": "SK_SKEW",
                        "SessionToken": "ST_SKEW",
                        "Expiration": soon_exp,
                    }
                }
            ),
        ),
        MockResponse(
            200,
            json.dumps(
                {
                    "Credentials": {
                        "AccessKeyId": "AK_ROTATED",
                        "SecretKey": "SK_ROTATED",
                        "SessionToken": "ST_ROTATED",
                        "Expiration": later_exp,
                    }
                }
            ),
        ),
    ]
    client = _make_client(session)
    client._identity_id = "us-east-2:id-1"
    rotated_event = asyncio.Event()
    created: list[Any] = []

    class SkewClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.on_connect: Any = None
            self.on_subscribe: Any = None
            self.on_message: Any = None
            self.on_disconnect: Any = None
            created.append(self)

        def ws_set_options(self, **kwargs: Any) -> None:
            pass

        def tls_set_context(self, ctx: Any) -> None:
            pass

        def connect(self, *args: Any, **kwargs: Any) -> None:
            pass

        def subscribe(self, topics: list[tuple[str, int]]) -> tuple[int, int]:
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)] * len(topics))
            if len(created) >= 2:
                rotated_event.set()
            return (mqtt.MQTT_ERR_SUCCESS, 1)

        def loop_start(self) -> None:
            self.on_connect(self, None, None, FakeReasonCode(False))

        def disconnect(self) -> None:
            pass

        def loop_stop(self) -> None:
            pass

    with (
        caplog.at_level(logging.WARNING, logger="custom_components.spider_venom.api"),
        patch("custom_components.spider_venom.api._MIN_ROTATION_DELAY", 0.05),
        patch("custom_components.spider_venom.api.mqtt.Client", SkewClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
    ):
        await client.async_start(lambda _doc: None)
        assert len(created) == 1
        await asyncio.wait_for(rotated_event.wait(), timeout=1.0)
        assert len(created) == 2
        assert "delaying rotation" in caplog.text
        await client.async_stop()


@pytest.mark.no_mock_mqtt_push
async def test_persistent_mqtt_invokes_on_reconnect_only_after_first_connect() -> None:
    """Verify on_reconnect fires on subsequent reconnects, not initial connect."""
    client = _make_client()
    client._credentials = _valid_creds()
    reconnect_calls = 0
    reconnected_event = asyncio.Event()
    created: list[Any] = []

    class ReconnectHookClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.on_connect: Any = None
            self.on_subscribe: Any = None
            self.on_message: Any = None
            self.on_disconnect: Any = None
            created.append(self)

        def ws_set_options(self, **kwargs: Any) -> None:
            pass

        def tls_set_context(self, ctx: Any) -> None:
            pass

        def connect(self, *args: Any, **kwargs: Any) -> None:
            pass

        def subscribe(self, topics: list[tuple[str, int]]) -> tuple[int, int]:
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)] * len(topics))
            return (mqtt.MQTT_ERR_SUCCESS, 1)

        def loop_start(self) -> None:
            self.on_connect(self, None, None, FakeReasonCode(False))

        def disconnect(self) -> None:
            pass

        def loop_stop(self) -> None:
            pass

    def on_reconnect() -> None:
        nonlocal reconnect_calls
        reconnect_calls += 1
        reconnected_event.set()

    with (
        patch("custom_components.spider_venom.api.mqtt.Client", ReconnectHookClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
        patch.object(client, "_async_get_credentials", return_value=_valid_creds()),
        patch.object(client, "_async_sleep_interruptible", return_value=None),
    ):
        await client.async_start(lambda _doc: None, on_reconnect=on_reconnect)
        assert reconnect_calls == 0

        created[0].on_disconnect(created[0], None, None, FakeReasonCode(True, "Unspecified error"))
        await asyncio.wait_for(reconnected_event.wait(), timeout=1.0)
        assert reconnect_calls == 1
        await client.async_stop()


@pytest.mark.no_mock_mqtt_push
async def test_persistent_mqtt_waits_for_rotation_and_skips_short_lived_fallback() -> None:
    """Verify persistent mode waits for readiness during reconnect/rotation and avoids _mqtt_shadow_call."""
    client = _make_client()
    client._credentials = _valid_creds()
    created: list[Any] = []
    ready_to_reconnect = asyncio.Event()

    class RotatingWaitClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.on_connect: Any = None
            self.on_subscribe: Any = None
            self.on_message: Any = None
            self.on_disconnect: Any = None
            created.append(self)

        def ws_set_options(self, **kwargs: Any) -> None:
            pass

        def tls_set_context(self, ctx: Any) -> None:
            pass

        def connect(self, *args: Any, **kwargs: Any) -> None:
            pass

        def subscribe(self, topics: list[tuple[str, int]]) -> tuple[int, int]:
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)] * len(topics))
            return (mqtt.MQTT_ERR_SUCCESS, 1)

        def publish(self, topic: str, payload: str, qos: int = 1) -> Any:
            token = json.loads(payload)["clientToken"]
            self.on_message(
                self,
                None,
                FakeMQTTMessage(
                    f"{topic}/accepted",
                    json.dumps({"clientToken": token, "state": {"reported": {"mainTemp": 288}}}).encode(),
                ),
            )
            return MagicMock(rc=mqtt.MQTT_ERR_SUCCESS)

        def loop_start(self) -> None:
            self.on_connect(self, None, None, FakeReasonCode(False))

        def disconnect(self) -> None:
            pass

        def loop_stop(self) -> None:
            pass

    async def gated_sleep(_delay: float) -> None:
        await ready_to_reconnect.wait()

    with (
        patch("custom_components.spider_venom.api.mqtt.Client", RotatingWaitClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
        patch.object(client, "_async_get_credentials", return_value=_valid_creds()),
        patch.object(client, "_async_sleep_interruptible", side_effect=gated_sleep),
        patch.object(client, "_mqtt_shadow_call", side_effect=AssertionError("fallback called")),
    ):
        await client.async_start(lambda _doc: None)

        # Simulate drop -> persistent mode is active but temporarily not ready
        created[0].on_disconnect(created[0], None, None, FakeReasonCode(True, "Unspecified error"))
        await asyncio.sleep(0.02)
        assert not client._ready

        # While still down past _PERSISTENT_READY_WAIT, fail fast without _mqtt_shadow_call
        with (
            patch("custom_components.spider_venom.api._PERSISTENT_READY_WAIT", 0.03),
            pytest.raises(SpiderVenomApiError, match="persistent connection is not ready"),
        ):
            await client.async_get_shadow()

        # When request arrives just before reconnect completes, it waits for _ready_event and succeeds
        shadow_task = asyncio.create_task(client.async_get_shadow())
        await asyncio.sleep(0.01)
        ready_to_reconnect.set()
        shadow = await asyncio.wait_for(shadow_task, timeout=1.0)
        assert shadow["state"]["reported"]["mainTemp"] == 288
        await client.async_stop()


@pytest.mark.no_mock_mqtt_push
async def test_mqtt_subscribe_publish_rc_errors_and_safe_call_soon_threadsafe() -> None:
    """Verify non-zero paho subscribe/publish return codes fail immediately and closed loops are ignored."""
    client = _make_client()
    client._credentials = _valid_creds()

    class BadSubscribeClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.on_connect: Any = None
            self.on_subscribe: Any = None
            self.on_message: Any = None
            self.on_disconnect: Any = None

        def ws_set_options(self, **kwargs: Any) -> None:
            pass

        def tls_set_context(self, ctx: Any) -> None:
            pass

        def connect(self, *args: Any, **kwargs: Any) -> None:
            pass

        def subscribe(self, topics: Any) -> tuple[int, int]:
            return (mqtt.MQTT_ERR_NO_CONN, 1)

        def loop_start(self) -> None:
            self.on_connect(self, None, None, FakeReasonCode(False))

        def disconnect(self) -> None:
            pass

        def loop_stop(self) -> None:
            pass

    with (
        patch("custom_components.spider_venom.api.mqtt.Client", BadSubscribeClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
        pytest.raises(SpiderVenomApiError, match="MQTT subscribe failed"),
    ):
        await client.async_get_shadow()

    class BadPublishClient(BadSubscribeClient):
        def subscribe(self, topics: Any) -> tuple[int, int]:
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)] * len(topics))
            return (mqtt.MQTT_ERR_SUCCESS, 1)

        def publish(self, topic: str, payload: str, qos: int = 1) -> Any:
            return MagicMock(rc=mqtt.MQTT_ERR_NO_CONN)

    with (
        patch("custom_components.spider_venom.api.mqtt.Client", BadPublishClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
        pytest.raises(SpiderVenomApiError, match="MQTT publish failed"),
    ):
        await client.async_get_shadow()

    # Persistent subscribe non-zero rc also fails without waiting for timeout
    class PersistentBadSubscribeClient(BadSubscribeClient):
        pass

    with (
        patch("custom_components.spider_venom.api.mqtt.Client", PersistentBadSubscribeClient),
        patch("custom_components.spider_venom.api.ssl.create_default_context", return_value=MagicMock()),
        pytest.raises(SpiderVenomApiError, match="MQTT subscribe failed"),
    ):
        await client.async_start(lambda _doc: None)

    closed_loop = MagicMock(spec=asyncio.AbstractEventLoop)
    closed_loop.call_soon_threadsafe.side_effect = RuntimeError("Event loop is closed")
    _safe_call_soon_threadsafe(closed_loop, lambda: None)
