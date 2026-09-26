"""Initial job and attempt tables.

Revision ID: 0001_initial
Revises:
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("task_type", sa.String(64), nullable=False),
        sa.Column("input", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("result", postgresql.JSONB()),
        sa.Column("last_error_code", sa.String(64)),
        sa.Column("last_error_message", sa.String(500)),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("timeout_seconds", sa.Integer(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("next_dispatch_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("current_attempt_id", postgresql.UUID(as_uuid=True)),
        sa.Column("worker_id", sa.String(128)),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("attempt_count >= 0 AND attempt_count <= max_attempts", name="valid_attempt_count"),
        sa.CheckConstraint("max_attempts BETWEEN 1 AND 10", name="valid_max_attempts"),
        sa.CheckConstraint("timeout_seconds BETWEEN 1 AND 3600", name="valid_timeout_seconds"),
    )
    op.create_index("ix_jobs_dispatch", "jobs", ["status", "available_at", "next_dispatch_at"])
    op.create_index("ix_jobs_lease", "jobs", ["status", "lease_expires_at"])
    op.create_index("ix_jobs_listing", "jobs", ["created_at", "id"])
    op.create_table(
        "job_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("jobs.id"), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.String(128), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True)),
        sa.Column("execution_deadline", sa.DateTime(timezone=True), nullable=False),
        sa.Column("outcome", sa.String(32)),
        sa.Column("error_code", sa.String(64)),
        sa.Column("error_message", sa.String(500)),
        sa.UniqueConstraint("job_id", "attempt_number", name="uq_attempt_number"),
    )
    op.create_index("ix_attempts_job_number", "job_attempts", ["job_id", "attempt_number"])


def downgrade():
    op.drop_table("job_attempts")
    op.drop_table("jobs")
