import asyncio
import websockets


def extract_sid(resp) -> str:
    if isinstance(resp, bytes):
        s = resp.decode("latin-1")
    elif isinstance(resp, str):
        s = resp
    else:
        raise TypeError(f"Unsupported type: {type(resp)}")
    start = s.find("") + 1
    end = s.find("", start)
    if start <= 0 or end == -1:
        raise ValueError("SID not found")
    return s[start:end]

TEMPLATES = [
    "/fs/fs3_sys_2{sid}(<-",
    "/fs/fs3_service{sid}(<-",
    "/fs/fs3_u_2_2{sid}(<-",
    "/fs/fs3_ul_2_107{sid}(<-",
]
def build_init_messages(resp_bytes: bytes) -> list[bytes]:
    sid = extract_sid(resp_bytes)
    messages = []
    for tpl in TEMPLATES:
        msg_str = tpl.format(sid=sid)
        messages.append(msg_str.encode("latin-1"))
    return messages

async def main():
    uri = "wss://p6tt2.fsdatacentre.com/WebSocketConnection-Secure"
    async with websockets.connect(
            uri,
            origin="https://www.tennis24.com",
            user_agent_header="Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            additional_headers={
                "Accept-Language": "en-US,en;q=0.5",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            },
    ) as ws:
        await ws.send(b'\x01\x01\x1e\x05\x08\x1e#PushClient WebSocket Client v5.1.3\x1e$\x01\x1e-\x02\x1e\x7f')
        first_response = await ws.recv()
        msgs = build_init_messages(first_response)
        for m in msgs:
            await ws.send(m)
        while True:
            msg = await ws.recv()
            handle_message(msg)


def handle_message(text: str):
    if "/fs/fs3_u_" in text:
        print(text)
        score_string = text.split("")[1].split('')[0]
        print(score_string)


asyncio.run(main())
