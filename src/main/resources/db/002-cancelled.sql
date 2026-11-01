-- Обновляет CHECK и для баз, созданных до появления CANCELLED.
ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_status_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_status_check
    CHECK (status IN ('PENDING','RUNNING','COMPLETED','FAILED','CANCELLED'));
