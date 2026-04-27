# manager.py
from typing import Dict, List
from fastapi import WebSocket

class ConnectionManager:
    def __init__(self):
        self.connections: Dict[int, List[WebSocket]] = {}

    async def connect(self, article_id: int, websocket: WebSocket) -> int:
        await websocket.accept()
        self.connections.setdefault(article_id, []).append(websocket)

        return len(self.connections[article_id])

    def disconnect(self, article_id: int, websocket: WebSocket):
        self.connections[article_id].remove(websocket)
        if not self.connections[article_id]:
            del self.connections[article_id]

    async def broadcast(self, article_id: int, message: dict):
        for ws in self.connections.get(article_id, []):
            await ws.send_json(message)

manager = ConnectionManager()