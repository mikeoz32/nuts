import contextlib

from nuts import Aggregate, Command, Event
from nuts.nuts import Nuts


@contextlib.asynccontextmanager
async def lifespan():
    yield {"service": "tasks"}


app = Nuts("tasks", lifespan=lifespan)


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
        return [Event("TaskAssigned", {"assignee_id": payload["assignee_id"]})]

    @Command("ChangeStatus")
    def change_status(self, payload, metadata=None):
        return [Event("TaskStatusChanged", {"status": payload["status"]})]

    @Event("TaskCreated")
    def apply_task_created(self, payload):
        self.state.update(payload)

    @Event("TaskAssigned")
    def apply_task_assigned(self, payload):
        self.state["assignee_id"] = payload["assignee_id"]

    @Event("TaskStatusChanged")
    def apply_task_status_changed(self, payload):
        self.state["status"] = payload["status"]


app.add_aggregate(TaskAggregate)


async def task_view_projection(
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


app.add_projection("task_views", "tasks", "TaskCreated", task_view_projection)
app.add_projection("task_views", "tasks", "TaskAssigned", task_view_projection)
app.add_projection("task_views", "tasks", "TaskStatusChanged", task_view_projection)
