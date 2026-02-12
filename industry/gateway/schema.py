"""
GraphQL schema sketch for the SaaS gateway.

This module describes the intended schema shape without binding to a specific
GraphQL library. Actual schema construction will be handled in the gateway
runtime (e.g., Strawberry or Ariadne).
"""


SCHEMA = """
type Query {
  me: ProfileView
  project(id: ID!): ProjectView
  projects: [ProjectView!]!
  tasks(projectId: ID!): [TaskView!]!
}

type Mutation {
  createTenant(input: CreateTenantInput!): TenantView
  registerUser(input: RegisterUserInput!): UserView
  createProject(input: CreateProjectInput!): ProjectView
  renameProject(input: RenameProjectInput!): ProjectView
  updateWikiPage(input: UpdateWikiInput!): WikiPageView
  createTask(input: CreateTaskInput!): TaskView
  assignTask(input: AssignTaskInput!): TaskView
  changeTaskStatus(input: ChangeTaskStatusInput!): TaskView
  updateResume(input: UpdateResumeInput!): ResumeView
  submitProposal(input: SubmitProposalInput!): ProposalView
  acceptProposal(input: AcceptProposalInput!): ContractView
  updateNotificationPreferences(input: UpdateNotificationPreferencesInput!): NotificationPreferenceView
}
"""
