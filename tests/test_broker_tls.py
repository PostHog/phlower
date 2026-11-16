"""Tests for TLS verification on rediss:// broker URLs.

kombu connects to a rediss:// broker without checking the certificate unless the
URL asks for it, so Phlower adds ssl_cert_reqs=required when the URL sets nothing.
"""

from __future__ import annotations

import ssl

import pytest
from kombu import Connection

from phlower.config import Config, with_tls_verification


@pytest.mark.parametrize(
    "url,expected",
    [
        ("rediss://broker:6379", "rediss://broker:6379?ssl_cert_reqs=required&ssl_check_hostname=true"),
        (
            "rediss://:secret@broker:6379/0?socket_timeout=5",
            "rediss://:secret@broker:6379/0?socket_timeout=5&ssl_cert_reqs=required&ssl_check_hostname=true",
        ),
        (
            "rediss://broker:6379?ssl_cert_reqs=CERT_REQUIRED",
            "rediss://broker:6379?ssl_cert_reqs=required&ssl_check_hostname=true",
        ),
        (
            "rediss://broker:6379?ssl_cert_reqs=required&ssl_check_hostname=false",
            "rediss://broker:6379?ssl_cert_reqs=required&ssl_check_hostname=false",
        ),
        ("rediss://broker:6379?ssl_cert_reqs=CERT_NONE", "rediss://broker:6379?ssl_cert_reqs=none"),
        ("redis://broker:6379/0", "redis://broker:6379/0"),
    ],
)
def test_with_tls_verification(url, expected):
    assert with_tls_verification(url) == expected


def test_config_applies_tls_verification(monkeypatch):
    monkeypatch.setenv("CELERY_BROKER_URL", "rediss://:secret@broker:6379")

    assert Config().broker_url == "rediss://:secret@broker:6379?ssl_cert_reqs=required&ssl_check_hostname=true"


def test_kombu_verifies_certificate_and_hostname():
    url = with_tls_verification("rediss://:secret@broker:6379")

    assert Connection(url).ssl == {"ssl_cert_reqs": ssl.CERT_REQUIRED, "ssl_check_hostname": True}
