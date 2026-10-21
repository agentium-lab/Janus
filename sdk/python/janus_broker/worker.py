"""JanusWorker: a high-level poll-process-ack loop for Python agents."""

import logging
import threading
import time
from typing import Callable, Optional

from .client import JanusClient, JanusAPIError, LeaseLostError
from .models import Task, AckRequest, NackRequest, TaskError

logger = logging.getLogger("janus_broker.worker")


class WorkerResult:
    """Result of processing a single task."""

    def __init__(self, task_id: str, success: bool, error: Optional[str] = None):
        self.task_id = task_id
        self.success = success
        self.error = error


class WorkerError(Exception):
    """Raised when the worker encounters an unrecoverable error."""


TaskHandler = Callable[..., "tuple[str, Optional[dict]]"]  # (task) or (task, progress)


ProgressFunc = Callable[[str, Optional[int], Optional[dict]], None]


class JanusWorker:
    """A high-level helper that runs a poll-process-ack loop.

    The application provides a handler function that receives (Task, progress) and returns
    (result_ref, token_usage). The Worker handles polling, task start/heartbeat,
    ACK/NACK, and progress reporting. Call progress("message", percent=50) inside
    the handler to stream real-time updates to subscribers; it is non-blocking,
    rate-limited (10/s), and failures are silently logged.

    Lease loss (default behavior): after 5 consecutive heartbeat failures the
    lease is presumed lost and the worker ABANDONS the current task — the
    handler is no longer driven (progress() raises LeaseLostError so a
    cooperative handler unwinds), the task is neither ACKed nor NACKed (the
    server re-queues it once the dead lease expires), and the loop moves on
    to the next poll. The optional on_lease_lost callback is a pure
    notification hook invoked right before the abandon; it cannot make the
    worker keep processing a task it no longer owns.

    Args:
        client: A JanusClient instance.
        agent_id: The agent ID this worker pulls for.
        mailbox_id: The mailbox to poll.
        poll_interval: Seconds between polls when queue is empty (default 2).
        heartbeat_interval: Seconds between task heartbeats (default 30).
        on_lease_lost: Notification hook called with the task ID right
            before that task is abandoned on presumed lease loss. Default
            None — the abandon happens with or without it.
    """

    def __init__(
        self,
        client: JanusClient,
        agent_id: str,
        mailbox_id: str,
        poll_interval: float = 2.0,
        heartbeat_interval: float = 30.0,
        error_backoff: float = 1.0,
        max_error_backoff: float = 30.0,
        agent_heartbeat_interval: float = 30.0,
        on_lease_lost: Optional[Callable[[str], None]] = None,
    ):
        self._client = client
        self._agent_id = agent_id
        self._mailbox_id = mailbox_id
        self._poll_interval = poll_interval
        self._heartbeat_interval = heartbeat_interval
        self._error_backoff = error_backoff
        self._max_error_backoff = max_error_backoff
        self._agent_heartbeat_interval = agent_heartbeat_interval
        self._on_lease_lost = on_lease_lost
        self._stop = threading.Event()

    def run(self, handler: TaskHandler) -> None:
        """Run the worker loop until stop() is called."""
        # Agent-liveness heartbeat: without it the sweeper offlines the
        # agent while it is actively processing (task heartbeats do not
        # count as agent heartbeats).
        agent_hb_stop = threading.Event()

        def _agent_heartbeat():
            while not agent_hb_stop.is_set() and not self._stop.is_set():
                try:
                    self._client.heartbeat_agent(self._agent_id)
                except Exception as e:
                    logger.debug("agent heartbeat (non-fatal): %s", e)
                agent_hb_stop.wait(self._agent_heartbeat_interval)

        agent_hb = threading.Thread(target=_agent_heartbeat, daemon=True)
        agent_hb.start()

        backoff = self._error_backoff
        try:
            while not self._stop.is_set():
                try:
                    self._process_one(handler)
                    backoff = self._error_backoff
                except Exception as e:
                    logger.error("janus worker: %s", e)
                    self._stop.wait(backoff)
                    backoff = min(backoff * 2, self._max_error_backoff)
        finally:
            agent_hb_stop.set()

    def stop(self) -> None:
        """Signal the worker to stop."""
        self._stop.set()

    def _process_one(self, handler: TaskHandler) -> None:
        result = self._client.pull_task(self._mailbox_id, self._agent_id)
        if result is None:
            self._stop.wait(self._poll_interval)
            return

        task = result.task
        lease_id = result.lease.lease_id
        attempt = result.lease.attempt

        try:
            self._client.start_task(task.id, lease_id, agent_id=self._agent_id)
        except JanusAPIError as e:
            raise WorkerError(f"start task {task.id}: {e}") from e

        # Heartbeat in a background thread.
        hb_stop = threading.Event()
        # Set when the lease is presumed lost (5 consecutive heartbeat
        # failures): the worker abandons this task — the handler stops
        # being driven and the task is neither ACKed nor NACKed.
        lease_lost = threading.Event()

        def _heartbeat():
            failures = 0
            while not hb_stop.is_set() and not self._stop.is_set():
                try:
                    self._client.heartbeat(task.id, lease_id, agent_id=self._agent_id)
                    failures = 0
                except Exception:
                    failures += 1
                    if failures >= 5:
                        logger.warning(
                            "janus worker: lease for task %s presumed lost after %d heartbeat failures; abandoning task",
                            task.id, failures,
                        )
                        # Notification hook only: fires right before the
                        # abandon, but cannot keep the worker on this task.
                        if self._on_lease_lost is not None:
                            try:
                                self._on_lease_lost(task.id)
                            except Exception:
                                pass
                        lease_lost.set()
                        return
                hb_stop.wait(self._heartbeat_interval)

        hb_thread = threading.Thread(target=_heartbeat, daemon=True)
        hb_thread.start()

        success = False
        error_msg = None

        def progress(message: str, percent: Optional[int] = None, data: Optional[dict] = None) -> None:
            # Lease lost: raise so cooperative handlers unwind instead of
            # burning compute on a task this worker no longer owns.
            if lease_lost.is_set():
                raise LeaseLostError(task.id)
            try:
                self._client.report_progress(task.id, message, percent=percent, data=data, agent_id=self._agent_id)
            except Exception:
                logger.debug("progress report failed for %s (non-fatal)", task.id)

        if lease_lost.is_set():
            # Lease lost before the handler even started: never invoke it.
            return

        try:
            import inspect as _inspect
            sig = _inspect.signature(handler)
            if len(sig.parameters) >= 2:
                result_ref, usage = handler(task, progress)
            else:
                result_ref, usage = handler(task)
            success = True
        except Exception as e:
            error_msg = str(e)

        hb_stop.set()

        if lease_lost.is_set():
            # Abandoned: the worker no longer owns this task. Neither ACK
            # nor NACK — the server re-queues it once the dead lease
            # expires and the next owner redoes the work.
            logger.info("janus worker: task %s abandoned (lease lost); skipping ack/nack", task.id)
            return

        if success:
            ack = AckRequest(lease_id=lease_id, attempt=attempt, result_ref=result_ref, agent_id=self._agent_id)
            if usage:
                ack.token_usage = usage
            self._client.ack_task(task.id, ack)
        else:
            self._client.nack_task(
                task.id,
                NackRequest(
                    lease_id=lease_id,
                    attempt=attempt,
                    retriable=True,
                    error=TaskError(code="HANDLER_ERROR", message=error_msg or "unknown"),
                    agent_id=self._agent_id,
                ),
            )
