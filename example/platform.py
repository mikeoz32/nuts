import contextlib
from typing import Dict

from nuts import Aggregate, Command, Event
from nuts.nuts import Nuts
from nuts.server.nsgi import NsgiServer


@contextlib.asynccontextmanager
async def lifespan():
    yield {"service": "platform"}


app = Nuts("platform", lifespan=lifespan)


class ProjectAggregate(Aggregate):
    aggregate_type = "projects"

    @Command("CreateProject")
    def create_project(self, payload, metadata=None):
        return [
            Event(
                "ProjectCreated",
                {
                    "name": payload["name"],
                    "owner_id": payload["owner_id"],
                    "description": payload.get("description", ""),
                },
            )
        ]

    @Command("UpdateWikiPage")
    def update_wiki_page(self, payload, metadata=None):
        return [
            Event(
                "WikiPageUpdated",
                {
                    "page_id": payload["page_id"],
                    "title": payload["title"],
                    "content": payload["content"],
                },
            )
        ]

    @Event("ProjectCreated")
    def apply_project_created(self, payload):
        self.state.update(
            {
                "name": payload["name"],
                "owner_id": payload["owner_id"],
                "description": payload["description"],
                "wiki": {},
                "tasks": [],
            }
        )

    @Event("WikiPageUpdated")
    def apply_wiki_page_updated(self, payload):
        wiki = self.state.setdefault("wiki", {})
        wiki[payload["page_id"]] = {
            "title": payload["title"],
            "content": payload["content"],
        }


class TaskAggregate(Aggregate):
    aggregate_type = "tasks"

    @Command("CreateTask")
    def create_task(self, payload, metadata=None):
        return [
            Event(
                "TaskCreated",
                {
                    "project_id": payload["project_id"],
                    "title": payload["title"],
                    "assignee_id": payload.get("assignee_id"),
                    "status": "open",
                },
            )
        ]

    @Command("AssignTask")
    def assign_task(self, payload, metadata=None):
        return [
            Event(
                "TaskAssigned",
                {
                    "assignee_id": payload["assignee_id"],
                },
            )
        ]

    @Event("TaskCreated")
    def apply_task_created(self, payload):
        self.state.update(payload)

    @Event("TaskAssigned")
    def apply_task_assigned(self, payload):
        self.state["assignee_id"] = payload["assignee_id"]


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


app.add_aggregate(ProjectAggregate)
app.add_aggregate(TaskAggregate)
app.add_aggregate(ProfileAggregate)


async def list_projects():
    return {"projects": []}


async def get_profile(user_id: str) -> Dict:
    return {"user_id": user_id}


app.add_method("list_projects", list_projects)
app.add_method("get_profile", get_profile)


if __name__ == "__main__":
    NsgiServer("tls://localhost:4222").run(app)
