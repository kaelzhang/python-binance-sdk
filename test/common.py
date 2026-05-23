import asyncio
import os
from pathlib import Path

from aiohttp import web
from dotenv import load_dotenv

from binance.common.utils import json_stringify


ACCOUNT_INFO = {
    'e': 'outboundAccountInfo',
    'E': 1499405658849,
    'm': 0,
    't': 0,
    'b': 0,
    's': 0,
}


MAX_PRINT = 150


def print_json(name, d):
    s = json_stringify(d)

    length = len(s)
    if length > MAX_PRINT:
        print(name, s[:MAX_PRINT], 'and %s more' % (length - MAX_PRINT))
    else:
        print(name, s)


PORT = 9081


class SocketServer:
    def __init__(self):
        self._started = False
        app = web.Application()
        app.add_routes([
            web.get('/stream', self._handler)
        ])

        self._app = app

        self._runner = web.AppRunner(app)

        self._delay = 0.2
        self._valid_json = True

    def start(self):
        self._started = True
        return self

    def no_timeout(self):
        self._delay = 0.05
        return self

    def invalid_json(self):
        self._valid_json = False
        return self

    def stop(self):
        self._started = False
        return self

    async def run(self):
        await self._runner.setup()
        site = web.TCPSite(self._runner, 'localhost', PORT)
        await site.start()

    async def shutdown(self):
        self.stop()
        await self._runner.cleanup()

    async def _handle(self, ws) -> None:
        if not self._started:
            await ws.close(code=1006)
            return

        # Drain incoming frames concurrently so aiohttp auto-responds to the
        # client's PING frames with PONGs. Without reading the socket, the
        # client's ping wait stalls for its full timeout and the connection is
        # wrongly treated as stale -- causing multi-second hangs per cycle.
        async def _drain():
            try:
                async for _ in ws:
                    pass
            except Exception:
                pass

        drain_task = asyncio.create_task(_drain())

        try:
            while self._started:
                if self._delay:
                    await asyncio.sleep(self._delay)

                if self._valid_json:
                    await ws.send_str('{"ok":true}')
                else:
                    await ws.send_str('{"ok":true')
        except Exception:
            # The client went away mid-send; stop quietly.
            pass
        finally:
            drain_task.cancel()

    async def _handler(self, request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        await self._handle(ws)

        return ws


_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_env_files():
    """Load environment variables from the project-root env file(s).

    Resolution order:
      1. the project-root ``.env`` file, if present;
      2. otherwise the first available ``.env.*`` file (sorted);
      3. inject the parsed keys into ``os.environ`` via python-dotenv
         (existing real environment values are preserved / not overridden).
    """
    env_path = _PROJECT_ROOT / '.env'

    if not env_path.is_file():
        candidates = sorted(_PROJECT_ROOT.glob('.env.*'))
        env_path = candidates[0] if candidates else None

    if env_path is not None and env_path.is_file():
        load_dotenv(dotenv_path=env_path)


def get_api_credentials():
    """Return ``(api_key, api_secret)`` for tests.

    Loads the project ``.env`` / ``.env.*`` files into ``os.environ`` first,
    then reads ``API_KEY`` / ``API_SECRET``. Returns ``(None, None)`` when
    they are not configured (so credential-dependent tests can self-skip).
    """
    load_env_files()
    return os.environ.get('API_KEY'), os.environ.get('API_SECRET')
