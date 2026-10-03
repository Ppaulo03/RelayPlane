-- Phase timeouts (e.g. VERIFY_CONNECTION) are measured from the start of the
-- current step, not from the creation of the whole operation.
ALTER TABLE operations ADD COLUMN step_started_at timestamptz NOT NULL DEFAULT now();
UPDATE operations SET step_started_at = updated_at;
