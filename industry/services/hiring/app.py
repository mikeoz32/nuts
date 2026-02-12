import contextlib

from nuts import Aggregate, Command, Event
from nuts.nuts import Nuts


@contextlib.asynccontextmanager
async def lifespan():
    yield {"service": "hiring"}


app = Nuts("hiring", lifespan=lifespan)


class ProposalAggregate(Aggregate):
    aggregate_type = "proposals"

    @Command("SubmitProposal")
    def submit_proposal(self, payload, metadata=None):
        return [
            Event(
                "ProposalSubmitted",
                {
                    "project_id": payload["project_id"],
                    "candidate_id": payload["candidate_id"],
                    "message": payload.get("message", ""),
                },
            )
        ]

    @Event("ProposalSubmitted")
    def apply_proposal_submitted(self, payload):
        self.state.update(payload)


class ContractAggregate(Aggregate):
    aggregate_type = "contracts"

    @Command("AcceptProposal")
    def accept_proposal(self, payload, metadata=None):
        return [
            Event(
                "ContractSigned",
                {
                    "proposal_id": payload["proposal_id"],
                    "start_date": payload.get("start_date"),
                },
            )
        ]

    @Event("ContractSigned")
    def apply_contract_signed(self, payload):
        self.state.update(payload)


app.add_aggregate(ProposalAggregate)
app.add_aggregate(ContractAggregate)


async def hiring_view_projection(
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
    "proposal_views", "proposals", "ProposalSubmitted", hiring_view_projection
)
app.add_projection(
    "contract_views", "contracts", "ContractSigned", hiring_view_projection
)
