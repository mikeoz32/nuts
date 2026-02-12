import contextlib

from nuts import Aggregate, Command, Event
from nuts.nuts import Nuts


@contextlib.asynccontextmanager
async def lifespan():
    yield {"service": "iam"}


app = Nuts("iam", lifespan=lifespan)


class TenantAggregate(Aggregate):
    aggregate_type = "tenants"

    @Command("CreateTenant")
    def create_tenant(self, payload, metadata=None):
        return [
            Event(
                "TenantCreated",
                {"name": payload["name"], "owner_id": payload["owner_id"]},
            )
        ]

    @Event("TenantCreated")
    def apply_tenant_created(self, payload):
        self.state.update(payload)


class UserAggregate(Aggregate):
    aggregate_type = "users"

    @Command("RegisterUser")
    def register_user(self, payload, metadata=None):
        return [
            Event(
                "UserRegistered",
                {
                    "email": payload["email"],
                    "display_name": payload.get("display_name", ""),
                },
            )
        ]

    @Event("UserRegistered")
    def apply_user_registered(self, payload):
        self.state.update(payload)


app.add_aggregate(TenantAggregate)
app.add_aggregate(UserAggregate)
