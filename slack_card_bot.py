from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from common import SLACK_BOT_TOKEN, SLACK_APP_TOKEN
from card_aggregation import register_card_handlers
from reservations import register_reservation_handlers

# =========================
# Slack App (Socket Mode)
# =========================
app = App(token=SLACK_BOT_TOKEN)

register_card_handlers(app)
register_reservation_handlers(app)


if __name__ == "__main__":
    print("[INFO] slack_card_bot starting (socket mode)...")
    SocketModeHandler(app, SLACK_APP_TOKEN).start()
