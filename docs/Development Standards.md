# KIVOR Development Standards

Version: 1.0

Status: Active

---

# Purpose

This document defines the official development standards for KIVOR.

Every implementation should follow these rules.

---

# 1. Development Process

Never start coding immediately.

Follow this sequence:

Understand

↓

Analyze

↓

Design

↓

Validate

↓

Implement

↓

Review

↓

Document

---

# 2. Coding Philosophy

Write code for the next developer.

Not only for today's feature.

Good code should be:

Simple

Readable

Predictable

Reusable

Maintainable

---

# 3. Backend Standards

Routes

Responsible only for HTTP.

Services

Responsible only for business logic.

Database

Accessed only through Services.

No SQL inside Routes.

No business rules inside main.py.

---

# 4. Frontend Standards

Pages contain presentation.

JavaScript contains interaction.

api.js contains HTTP communication.

Business rules remain in the backend.

---

# 5. Naming Conventions

Python

ventas_lyl.py

ventas_lyl_service.py

ventas_lyl_schema.py

JavaScript

ventas-lyl.js

CSS

ventas-lyl.css

HTML

ventas-lyl.html

---

# 6. API Standards

Successful response

{
    "success": true,
    "data": {}
}

Error response

{
    "success": false,
    "message": ""
}

Maintain consistent response structures.

---

# 7. SQL Standards

Always use parameterized queries.

Never concatenate SQL strings.

Always validate tenant_schema.

Keep SQL readable.

---

# 8. Error Handling

Raise domain exceptions.

Translate exceptions at Route level.

Never hide unexpected errors.

Provide meaningful messages.

---

# 9. Documentation

Every significant implementation should update documentation.

Architecture changes

↓

Architecture.md

Business rules

↓

Documento Vivo

Technical implementation

↓

Technical Catalog

---

# 10. Code Reviews

Review for:

Architecture

Business rules

Security

Performance

Maintainability

Documentation

---

# 11. Refactoring Rules

Do not refactor unrelated modules.

Prefer incremental improvements.

Avoid unnecessary rewrites.

Keep commits focused.

---

# 12. Reusable Components

Before creating a new component:

Search existing ones.

Extend when appropriate.

Create only if reuse is impossible.

---

# 13. Security

Validate authentication.

Validate authorization.

Validate tenant ownership.

Validate input.

Never trust frontend data.

---

# 14. Database

Keep infrastructure inside core.

Keep business inside tenant schemas.

Avoid duplicated information.

Prefer normalization.

---

# 15. Performance

Avoid unnecessary queries.

Reuse loaded information.

Minimize network requests.

Optimize only after measuring.

---

# 16. Testing

Every implementation should verify:

Normal scenario

Boundary conditions

Invalid data

Permission validation

Error handling

---

# 17. Git

Small commits.

Meaningful messages.

No unrelated changes.

Never commit generated files unless requested.

---

# 18. Technical Debt

When found:

Document it.

Classify it.

Explain the impact.

Never hide it.

---

# 19. Quality Checklist

Before considering a feature complete:

✓ Architecture respected

✓ Business rules respected

✓ Documentation updated

✓ No duplicated logic

✓ Consistent naming

✓ Security reviewed

✓ Errors handled

✓ Technical debt evaluated

---

# 20. Golden Rule

Every change should leave KIVOR in a better state than before.

Never sacrifice long-term quality for short-term speed.