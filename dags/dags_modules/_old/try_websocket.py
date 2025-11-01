# tennis24_ws_client.py
import zlib
import traceback
from websocket import WebSocketApp


WS_URL = "wss://p6tt2.fsdatacentre.com/WebSocketConnection-Secure"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0",
    "Origin": "https://www.tennis24.com",
}

RS = chr(30)  # 0x1E — record separator
FIELD_SEP = "¬"  # 0xAC
KV_SEP = "÷"     # 0xF7


def decompress(raw: bytes) -> str:
    """Пробуем распаковать бинарное сообщение FlashScore-feed."""
    try:
        return zlib.decompress(raw, -15).decode("utf-8", "replace")
    except Exception:
        try:
            return raw.decode("utf-8", "replace")
        except Exception:
            return repr(raw)


def parse_block(block: str) -> dict:
    """Парсит структуру SA÷...¬AA÷...¬."""
    data = {}
    block = block.strip("~\x00 ")
    if FIELD_SEP in block:
        for part in block.split(FIELD_SEP):
            if KV_SEP in part:
                k, v = part.split(KV_SEP, 1)
                data[k] = v
    return data


def on_message(ws, message):
    try:
        text = decompress(message) if isinstance(message, (bytes, bytearray)) else message
        for b in text.split(RS):
            b = b.strip()
            if not b:
                continue
            parsed = parse_block(b)
            if parsed:
                print("📊", parsed)
            else:
                print("🧩 raw:", b[:100])
    except Exception:
        traceback.print_exc()


def on_open(ws):
    print("✅ Connected to feed")
    # tennis24 feed обычно не требует явной подписки — поток идёт сам.
    # Если в DevTools → Frames видно исходящее сообщение — добавьте его здесь:
    # ws.send('{"type":"subscribe","sports":["tennis"]}')


def on_error(ws, error):
    print("[!] Error:", error)


def on_close(ws, code, reason):
    print(f"❌ Closed: code={code}, reason={reason}")


def main():
    header_lines = [f"{k}: {v}" for k, v in HEADERS.items()]
    ws = WebSocketApp(
        WS_URL,
        header=header_lines,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever(ping_interval=20, ping_timeout=10)


if __name__ == "__main__":
    main()
