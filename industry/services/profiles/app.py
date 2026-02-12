import contextlib

from nuts import Aggregate, Command, Event
from nuts.nuts import Nuts


@contextlib.asynccontextmanager
async def lifespan():
    yield {"service": "profiles"}


app = Nuts("profiles", lifespan=lifespan)


class ProfileAggregate(Aggregate):
    aggregate_type = "profiles"

    @Command("CreateProfile")
    def create_profile(self, payload, metadata=None):
        return [
            Event(
                "ProfileCreated",
                {
                    "user_id": payload["user_id"],
                    "display_name": payload["display_name"],
                    "headline": payload.get("headline", ""),
                },
            )
        ]

    @Command("UpdateResume")
    def update_resume(self, payload, metadata=None):
        return [
            Event(
                "ResumeUpdated",
                {
                    "summary": payload.get("summary", ""),
                    "skills": payload.get("skills", []),
                    "experience": payload.get("experience", []),
                },
            )
        ]

    @Event("ProfileCreated")
    def apply_profile_created(self, payload):
        self.state.update(payload)

    @Event("ResumeUpdated")
    def apply_resume_updated(self, payload):
        resume = self.state.setdefault("resume", {})
        resume.update(payload)


app.add_aggregate(ProfileAggregate)
