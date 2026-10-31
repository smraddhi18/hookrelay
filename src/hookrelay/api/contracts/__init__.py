"""Request/response models (the public JSON contract) for the tenant and admin APIs.

Kept separate from the ORM models on purpose: the database can evolve (columns, encryption,
soft-delete flags) without changing what clients see, and secrets never leak through an
accidental ``from_attributes`` on a model that has them.
"""
