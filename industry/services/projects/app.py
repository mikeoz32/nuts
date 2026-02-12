import contextlib

from nuts import Aggregate, Command, Event
from nuts.nuts import Nuts


@contextlib.asynccontextmanager
async def lifespan():
    yield {"service": "projects"}


app = Nuts("projects", lifespan=lifespan)


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

    @Command("RenameProject")
    def rename_project(self, payload, metadata=None):
        return [Event("ProjectRenamed", {"name": payload["name"]})]

    @Event("ProjectCreated")
    def apply_project_created(self, payload):
        self.state.update(payload)

    @Event("ProjectRenamed")
    def apply_project_renamed(self, payload):
        self.state["name"] = payload["name"]


class WikiPageAggregate(Aggregate):
    aggregate_type = "wiki_pages"

    @Command("UpdateWikiPage")
    def update_wiki_page(self, payload, metadata=None):
        return [
            Event(
                "WikiPageUpdated",
                {
                    "project_id": payload["project_id"],
                    "title": payload["title"],
                    "content": payload["content"],
                },
            )
        ]

    @Event("WikiPageUpdated")
    def apply_wiki_page_updated(self, payload):
        self.state.update(payload)


app.add_aggregate(ProjectAggregate)
app.add_aggregate(WikiPageAggregate)


async def project_view_projection(
    payload,
    projection,
    metadata=None,
    aggregate_id=None,
    sequence=None,
    aggregate_type=None,
):
    view = projection or {"id": aggregate_id, "type": aggregate_type}
    if aggregate_type == "projects":
        if "name" in payload:
            view["name"] = payload["name"]
        if "owner_id" in payload:
            view["owner_id"] = payload["owner_id"]
        if "description" in payload:
            view["description"] = payload["description"]
    if aggregate_type == "wiki_pages":
        view.setdefault("wiki", {})
        view["wiki"][payload["title"]] = payload["content"]
    return view


app.add_projection("project_views", "projects", "ProjectCreated", project_view_projection)
app.add_projection("project_views", "projects", "ProjectRenamed", project_view_projection)
app.add_projection(
    "project_views", "wiki_pages", "WikiPageUpdated", project_view_projection
)
