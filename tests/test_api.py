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
        def __init__(self, api_version: Any, *, client_id: str, transport: str) -> None:
            assert api_version is mqtt.CallbackAPIVersion.VERSION2
            assert transport == "websockets"
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

        def subscribe(self, topics: list[tuple[str, int]]) -> None:
            assert topics[0][0].endswith("/accepted")
            assert topics[1][0].endswith("/rejected")
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)])

        def publish(self, topic: str, payload: str, qos: int = 1) -> None:
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

        def subscribe(self, topics: Any) -> None:
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)])

        def publish(self, topic: str, payload: str, qos: int = 1) -> None:
            token = json.loads(payload)["clientToken"]
            self.on_message(
                self,
                None,
                FakeMQTTMessage(
                    f"{topic}/rejected",
                    json.dumps({"clientToken": token, "code": 400, "message": "Invalid payload"}).encode(),
                ),
            )

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
                raise mqtt.WebsocketConnectionError("HTTP 403 Forbidden")

        def subscribe(self, topics: Any) -> None:
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)])

        def publish(self, topic: str, payload: str, qos: int = 1) -> None:
            token = json.loads(payload)["clientToken"]
            self.on_message(
                self,
                None,
                FakeMQTTMessage(
                    f"{topic}/accepted",
                    json.dumps({"clientToken": token, "state": {"reported": {"mainTemp": 230}}}).encode(),
                ),
            )

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
    assert redacted["device"]["thingName"] == "venom-1"

    assert _redacted_body("<html>err</html>", redact_non_json=True) == "<non-JSON response body omitted>"
    assert _redacted_body(json.dumps({"data": "x" * 5000})).endswith("...<truncated>")


@pytest.mark.no_mock_mqtt_push
async def test_persistent_mqtt_push_subscriptions_requests_and_stop() -> None:
    """Verify persistent MQTT mode subscribes to 5 topics, reuses socket, delivers pushes, and stops."""
    client = _make_client()
    client._credentials = _valid_creds()
    created_clients: list[Any] = []
    subscribed_topics: list[list[str]] = []
    pushed_docs: list[dict[str, Any]] = []

    class PersistentFakeClient:
        def __init__(self, api_version: Any, *, client_id: str, transport: str) -> None:
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

        def subscribe(self, topics: list[tuple[str, int]]) -> None:
            subscribed_topics.append([t for t, _qos in topics])
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)] * len(topics))

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

        def subscribe(self, topics: list[tuple[str, int]]) -> None:
            self.on_subscribe(self, None, 1, [FakeReasonCode(False)] * len(topics))

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

        # Simulate unexpected auth disconnect -> clears credentials and reconnects (#3)
        created_clients[1].on_disconnect(
            created_clients[1], None, None, FakeReasonCode(True, "Not authorized")
        )
        await asyncio.wait_for(reconnect_event.wait(), timeout=2.0)
        assert len(created_clients) >= 3
        assert client._credentials is not None
        assert client._credentials.access_key_id == "AK3"

        await client.async_stop()
