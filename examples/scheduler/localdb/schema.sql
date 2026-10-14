CREATE TABLE app_meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_task_id INTEGER NOT NULL CHECK (last_task_id >= 0),
    succeeded_count INTEGER NOT NULL CHECK (succeeded_count >= 0),
    failed_count INTEGER NOT NULL CHECK (failed_count >= 0)
) STRICT;

CREATE TABLE tasks (
    task_id INTEGER PRIMARY KEY CHECK (task_id > 0),
    state TEXT NOT NULL CHECK (state IN ('pending', 'assigned', 'running', 'succeeded', 'failed')),
    seed TEXT NOT NULL CHECK (length(seed) = 64),
    duration_ms INTEGER NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    executor_replica_id INTEGER,
    executor_node TEXT,
    created_index INTEGER NOT NULL,
    updated_index INTEGER NOT NULL,
    started_index INTEGER,
    finished_index INTEGER,
    result TEXT,
    error_text TEXT,
    CHECK ((state = 'pending') = (executor_replica_id IS NULL))
) STRICT;

CREATE INDEX tasks_state_id ON tasks(state, task_id);
CREATE INDEX tasks_executor_state ON tasks(executor_replica_id, state, task_id);
CREATE INDEX tasks_terminal_history ON tasks(finished_index, task_id)
    WHERE state IN ('succeeded', 'failed');
