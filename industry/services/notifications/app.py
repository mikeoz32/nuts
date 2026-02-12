import contextlib

from nuts import Aggregate, Command, Event
from nuts.nuts import Nuts


@contextlib.asynccontextmanager
async def lifespan():
    yield {"service": "notifications"}


app = Nuts("notifications", lifespan=lifespan)


class NotificationPreferenceAggregate(Aggregate):
    aggregate_type = "notification_prefs"

    @Command("UpdatePreferences")
    def update_preferences(self, payload, metadata=None):
        return [
            Event(
                "NotificationPreferenceUpdated",
                {
                    "user_id": payload["user_id"],
                    "channels": payload.get("channels", []),
                },
            )
        ]

    @Event("NotificationPreferenceUpdated")
    def apply_preference_updated(self, payload):
        self.state.update(payload)


app.add_aggregate(NotificationPreferenceAggregate)
