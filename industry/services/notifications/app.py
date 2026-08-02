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


async def notification_preference_projection(
    payload,
    projection,
    metadata=None,
    aggregate_id=None,
    sequence=None,
    aggregate_type=None,
):
    view = projection or {"id": aggregate_id, "type": aggregate_type}
    view.update(payload)
    return view


app.add_projection(
    "notification_prefs",
    "notification_prefs",
    "NotificationPreferenceUpdated",
    notification_preference_projection,
)
