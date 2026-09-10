---
name: request-refactor-plan
description: Create a detailed refactor plan with tiny commits via user interview, then file it as a GitHub issue. Use when user wants to plan a refactor, create a refactoring RFC, or break a refactor into safe incremental steps.
---

This skill will be invoked when the user wants to create a refactor request. You should go through the steps below. You may skip steps if you don't consider them necessary.

1. Start from the problem, constraints, and proposed solutions already supplied. Inspect available context before asking; request only missing information that materially changes the scope or intended behavior, rather than asking the user to repeat a full description.

2. Explore the repo to verify their assertions and understand the current state of the codebase.

3. Present relevant alternatives when they affect a real trade-off. Reuse decisions already made; do not require an alternatives interview for settled choices.

4. Ask about unresolved business, compatibility, or hard-to-reverse decisions. Resolve routine implementation choices from the code and project conventions. Use a detailed interview when the user requests one.

5. Hammer out the exact scope of the implementation. Work out what you plan to change and what you plan not to change.

6. Inspect existing test coverage and propose verification for the affected behavior. If coverage is insufficient, identify the gap and include a concrete testing approach; ask only about missing acceptance criteria or additional authorization. Preserve any explicit test-seam confirmation requirement.

7. Break the implementation into a plan of tiny commits. Remember Martin Fowler's advice to "make each refactoring step as small as possible, so that you can always see the program working."

8. Create a GitHub issue with the refactor plan. Use the following template for the issue description:

<refactor-plan-template>

## Problem Statement

The problem that the developer is facing, from the developer's perspective.

## Solution

The solution to the problem, from the developer's perspective.

## Commits

A LONG, detailed implementation plan. Write the plan in plain English, breaking down the implementation into the tiniest commits possible. Each commit should leave the codebase in a working state.

## Decision Document

A list of implementation decisions that were made. This can include:

- The modules that will be built/modified
- The interfaces of those modules that will be modified
- Technical clarifications from the developer
- Architectural decisions
- Schema changes
- API contracts
- Specific interactions

Do NOT include specific file paths or code snippets. They may end up being outdated very quickly.

## Testing Decisions

A list of testing decisions that were made. Include:

- A description of what makes a good test (only test external behavior, not implementation details)
- Which modules will be tested
- Prior art for the tests (i.e. similar types of tests in the codebase)

## Out of Scope

A description of the things that are out of scope for this refactor.

## Further Notes (optional)

Any further notes about the refactor.

</refactor-plan-template>
