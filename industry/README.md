# Industry (SaaS platform implementation)

This folder holds the in-progress implementation of the SaaS platform described
in `docs/saas_implementation_plan.md`.

## Structure
- `industry/gateway/` - GraphQL API gateway (planned).
- `industry/services/` - Domain services (IAM, profiles, projects, tasks, hiring, notifications).

Each service will contain its own Nuts app, aggregates, and projection handlers.
