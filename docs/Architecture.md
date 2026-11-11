# KIVOR Architecture

Version: 1.0

Status: Active

---

# 1. Purpose

This document describes the architectural design of KIVOR.

It explains how the different layers interact, the responsibilities of each module, and the rules that preserve the long-term maintainability of the platform.

This document describes architecture.

Business rules belong to the functional documentation.

Implementation details belong to the Technical Catalog.

---

# 2. Vision

KIVOR is a modular platform for managing service businesses.

The current implementation has been developed using a completely decoupled architecture where:

- Frontend
- Backend
- Database

evolve independently while respecting a common contract.

The project is designed for long-term evolution.

Every architectural decision must reduce coupling and improve maintainability.

---

# 3. High-Level Architecture

                    Browser
                       │
                       ▼
              HTML / CSS / JavaScript
                       │
                       ▼
                   api.js
                       │
                       ▼
                 FastAPI Routes
                       │
                       ▼
                  Business Services
                       │
                       ▼
                  PostgreSQL

Each layer has a single responsibility.

---

# 4. Repository Structure

KIVOR consists of two independent repositories.

Backend

- FastAPI
- Python
- PostgreSQL

Frontend

- HTML
- CSS
- Vanilla JavaScript

Neither repository should contain responsibilities belonging to the other.

---

# 5. Backend Architecture

Pattern:

Route

↓

Service

↓

Database

Routes expose REST APIs.

Services implement business rules.

Database access belongs exclusively to Services.

---

# 6. Frontend Architecture

Pattern:

HTML

↓

JavaScript

↓

api.js

↓

REST API

Every HTTP request must pass through api.js.

Pages never communicate directly with the backend.

---

# 7. Multi-Tenant Architecture

KIVOR supports multiple organizations through tenant schemas.

Core schema

↓

Tenant schema

Each tenant contains only business data.

Infrastructure belongs to the core schema.

Business logic must preserve tenant isolation.

---

# 8. Database Philosophy

Core

Infrastructure

Authentication

Organizations

Users

Sessions

Menus

Tenant

Business

Sales

Customers

Appointments

Services

Analytics

Business functionality should never be moved into core unless it becomes cross-tenant infrastructure.

---

# 9. Communication Flow

User

↓

Screen

↓

JavaScript

↓

api.js

↓

Route

↓

Service

↓

PostgreSQL

↓

Response

↓

Screen

This flow should never be bypassed.

---

# 10. Component Philosophy

Components should be:

Reusable

Independent

Configurable

Documented

Future reusable components include:

- GRID_KIVOR
- DATE_KIVOR
- BUTTON_KIVOR
- MODAL_KIVOR
- ALERT_KIVOR
- SELECT_KIVOR

---

# 11. Module Independence

Every functional module should evolve independently.

Examples:

Authentication

Customer Express

Analytics

Sales

Agenda

Customers

Professionals

Reports

Each module should expose only the APIs it owns.

---

# 12. Documentation Hierarchy

Architecture.md

↓

Documento Vivo

↓

Technical Catalog

↓

Code

The documentation explains why.

The code explains how.

---

# 13. Technical Debt Strategy

Technical debt should never be hidden.

When identified it must be classified as:

High

Medium

Low

Each item should describe:

Current implementation

Risk

Recommended solution

Impact

Priority

---

# 14. Growth Strategy

KIVOR should grow by adding modules, not by increasing complexity.

Preferred approach:

New module

↓

New Route

↓

New Service

↓

Existing infrastructure

instead of

Large generic services

Large generic screens

Duplicated logic

---

# 15. Architectural Principles

Architecture has priority over implementation speed.

Consistency has priority over shortcuts.

Reusability has priority over duplication.

Simplicity has priority over cleverness.

Documentation evolves together with the code.
