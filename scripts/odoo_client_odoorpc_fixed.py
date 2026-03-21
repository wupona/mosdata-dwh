# -*- coding: utf-8 -*-

import time
import random
import ssl
import urllib.error
import http.client
import odoorpc


class OdooClient:
    """
    Client Odoo RPC robuste avec retries, backoff exponentiel
    et gestion des erreurs réseau / SSL / proxy.
    """

    def __init__(
        self,
        host: str,
        db: str,
        user: str,
        password: str,
        port: int = 443,
        protocol: str = "jsonrpc+ssl",
        timeout: int = 120,
    ):
        self.host = host
        self.db = db
        self.user = user
        self.password = password
        self.port = port
        self.protocol = protocol
        self.timeout = timeout
        self.odoo = None

    # ------------------------------------------------------------------
    # Connexion
    # ------------------------------------------------------------------
    def connect(self):
        """
        Initialise la connexion OdooRPC
        """
        self.odoo = odoorpc.ODOO(
            self.host,
            protocol=self.protocol,
            port=self.port,
            timeout=self.timeout,
        )
        self.odoo.login(self.db, self.user, self.password)

    # ------------------------------------------------------------------
    # Exécution RPC robuste
    # ------------------------------------------------------------------
    def execute(self, model, method, *args, **kwargs):
        """
        Exécute une méthode Odoo avec retries/backoff
        sur erreurs réseau / SSL / proxy.
        """
        if not self.odoo:
            raise ConnectionError("Client non connecté. Appelez connect() d'abord.")

        max_retries = kwargs.pop("_max_retries", 6)
        base_sleep = kwargs.pop("_base_sleep", 1.0)

        RETRYABLE = (
            urllib.error.URLError,
            urllib.error.HTTPError,
            ConnectionResetError,
            ssl.SSLError,
            http.client.RemoteDisconnected,
            TimeoutError,
        )

        def is_retryable(exc: Exception) -> bool:
            if isinstance(exc, urllib.error.HTTPError):
                return exc.code in (429, 502, 503, 504)

            if isinstance(exc, urllib.error.URLError):
                msg = str(exc).lower()
                return any(k in msg for k in (
                    "connection reset",
                    "reset by peer",
                    "timed out",
                    "timeout",
                    "ssl",
                    "tls",
                    "handshake",
                    "temporarily unavailable",
                ))

            if isinstance(exc, (
                ConnectionResetError,
                ssl.SSLError,
                http.client.RemoteDisconnected,
                TimeoutError,
            )):
                return True

            msg = str(exc).lower()
            return any(k in msg for k in (
                "connection reset",
                "reset by peer",
                "timeout",
                "ssl",
                "handshake",
                "502",
                "503",
                "504",
                "429",
            ))

        last_exc = None

        for attempt in range(1, max_retries + 1):
            try:
                model_obj = self.odoo.env[model]

                if method == "search_read":
                    domain = args[0] if args else []
                    return model_obj.search_read(domain, **kwargs)

                if method == "search_count":
                    domain = args[0] if args else []
                    return model_obj.search_count(domain)

                func = getattr(model_obj, method)
                return func(*args, **kwargs)

            except RETRYABLE as exc:
                last_exc = exc

                if not is_retryable(exc) or attempt == max_retries:
                    raise RuntimeError(
                        f"Erreur {model}.{method}: {exc}"
                    ) from exc

                sleep_s = min(
                    60,
                    base_sleep * (2 ** (attempt - 1)) + random.random()
                )

                print(
                    f"⚠️  Odoo RPC transient "
                    f"({model}.{method}) "
                    f"{type(exc).__name__}: {exc} | "
                    f"retry {attempt}/{max_retries} in {sleep_s:.1f}s"
                )

                time.sleep(sleep_s)

            except Exception as exc:
                raise RuntimeError(
                    f"Erreur {model}.{method}: {exc}"
                ) from exc

        raise RuntimeError(
            f"Erreur {model}.{method} après {max_retries} tentatives: {last_exc}"
        ) from last_exc
