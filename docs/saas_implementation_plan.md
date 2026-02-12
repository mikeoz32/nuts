# SaaS Implementation Plan (Nuts-based platform)

## Goal
Build a multi-tenant SaaS platform where customers can create projects, hire people, manage project wikis and task tracking, and maintain user profiles with resumes. The platform is built on **Nuts** with event sourcing and a **GraphQL API Gateway**.

---

## Service Catalog (bounded contexts)

### 1) **Identity & Access Service (IAM)**
**Purpose:** authentication, authorization, tenant boundaries, roles.
- **Aggregates**
  - `Tenant` (create tenant, invite members, manage billing plan)
  - `User` (register, verify email, lock/unlock, assign roles)
  - `Membership` (user ↔ tenant role)
- **Events**
  - `TenantCreated`, `UserRegistered`, `MemberInvited`, `RoleAssigned`, `MemberRemoved`
- **Key read models**
  - `TenantSummary`, `UserProfileBasic`, `MembershipList`
- **External integrations**
  - Email provider (invitations, password resets)

### 2) **Profiles Service**
**Purpose:** user profiles and resumes (skills, experience).
- **Aggregates**
  - `Profile` (display name, headline, resume sections)
- **Events**
  - `ProfileCreated`, `ResumeUpdated`, `SkillAdded`, `ExperienceUpdated`
- **Read models**
  - `ProfileView`, `ResumeView`

### 3) **Projects Service**
**Purpose:** project creation, wiki, and project metadata.
- **Aggregates**
  - `Project` (name, description, owner, status)
  - `WikiPage` (project wiki pages)
- **Events**
  - `ProjectCreated`, `ProjectRenamed`, `ProjectArchived`
  - `WikiPageUpdated`
- **Read models**
  - `ProjectView`, `ProjectWikiView`

### 4) **Tasks Service**
**Purpose:** task tracking, assignments, statuses.
- **Aggregates**
  - `Task` (title, description, assignee, status, due date)
  - `TaskBoard` (optional: board-level workflow)
- **Events**
  - `TaskCreated`, `TaskAssigned`, `TaskStatusChanged`, `TaskCommented`
- **Read models**
  - `TaskView`, `TaskListByProject`

### 5) **Hiring Service**
**Purpose:** matching, proposals, and hiring pipeline.
- **Aggregates**
  - `Proposal` (candidate → project)
  - `Contract` (accepted proposal)
- **Events**
  - `ProposalSubmitted`, `ProposalAccepted`, `ContractSigned`, `ContractClosed`
- **Read models**
  - `ProposalList`, `ContractView`

### 6) **Notifications Service**
**Purpose:** email/in-app notifications, subscriptions.
- **Aggregates**
  - `NotificationPreference`
- **Events**
  - `NotificationQueued`, `NotificationSent`
- **Read models**
  - `NotificationInbox`

### 7) **Billing Service** (optional in v1, but reserved)
**Purpose:** subscription, invoices, payment status.
- **Aggregates**
  - `Subscription`, `Invoice`
- **Events**
  - `SubscriptionStarted`, `PaymentFailed`, `InvoiceIssued`

---

## API Gateway (GraphQL)

### Responsibilities
- Authenticate user (JWT/OAuth).
- Resolve tenant scope.
- Aggregate data from read models (projections).
- Provide mutation endpoints that map to ES commands.
- Validate request shape and enforce policies.

### Example GraphQL schema (MVP)
```graphql
type Query {
  me: ProfileView
  project(id: ID!): ProjectView
  projects: [ProjectView!]!
  tasks(projectId: ID!): [TaskView!]!
}

type Mutation {
  createProject(input: CreateProjectInput!): ProjectView
  updateWikiPage(input: UpdateWikiInput!): WikiPageView
  createTask(input: CreateTaskInput!): TaskView
  assignTask(input: AssignTaskInput!): TaskView
  updateResume(input: UpdateResumeInput!): ResumeView
}
```

### GraphQL → Nuts ES mapping
- Each mutation emits a **command** to the appropriate service (via NATS).
- Queries read from **projection stores** (KV / database).

---

## Cross-cutting Requirements
- **Multi-tenancy**: every command + event includes `tenant_id` in metadata.
- **Authorization**: enforced in GraphQL gateway and validated in domain handlers.
- **Observability**: structured logging + metrics per command.
- **Schema evolution**: versioned events with migration hooks.
- **Idempotency**: commands include `command_id` / `correlation_id`.

---

## Implementation Epics

### Epic 1: Foundational Platform Setup
- **Goal:** establish service skeletons and shared libraries.
  - Service templates (Nuts + ES feature + projections).
  - Shared “domain contracts” library for event envelopes.

### Epic 2: GraphQL API Gateway
- **Goal:** central API for UI + integrations.
  - GraphQL schema + resolvers per domain.
  - Command client (NATS) + read-model adapters.
  - Auth, tenant resolution, request context.

### Epic 3: IAM Service
- **Goal:** authenticate users and manage tenants/roles.
  - Tenant and User aggregates.
  - Invite flow and basic role policy.

### Epic 4: Profiles Service
- **Goal:** user profiles and resumes.
  - Profile aggregate.
  - Projections for profile views.

### Epic 5: Projects + Wiki
- **Goal:** project creation and wiki content.
  - Project + WikiPage aggregates.
  - Projections for project and wiki views.

### Epic 6: Tasks Service
- **Goal:** task tracking and assignments.
  - Task aggregate.
  - Projections for project task lists.

### Epic 7: Hiring Service
- **Goal:** proposals and contracts.
  - Proposal + Contract aggregates.
  - Projections for pipeline views.

### Epic 8: Notifications
- **Goal:** notifications and subscriptions.
  - Notification queue.
  - Integration with email provider.

---

## Suggested MVP Cut
1. IAM (tenant/user/membership).
2. Projects + Wiki.
3. Tasks (basic).
4. Profiles.
5. GraphQL gateway.

---

## Infrastructure Plan
- **NATS + JetStream** for event store.
- **JetStream KV** for projection checkpoints / snapshots.
- **Postgres** (optional, if projections become complex).
- **Gateway runtime**: FastAPI/Starlette + Strawberry GraphQL or Ariadne.

---

## Open Questions
- Do we want **single stream** per service or per aggregate type?
- Which storage for read models (KV vs Postgres)?
- How strict should tenant isolation be (single NATS namespace vs tenant metadata)?
