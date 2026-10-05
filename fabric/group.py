import threading
from queue import Empty, Queue

from invoke.exceptions import Failure
from invoke.util import ExceptionHandlingThread

from .connection import Connection
from .exceptions import GroupException


#: Status strings for individual items in a `.GroupResult` manifest.
#:
#: - ``SUCCESS``: the called method returned normally.
#: - ``COMMAND_FAILED``: the remote command exited uncleanly (e.g.
#:   `.UnexpectedExit`); the connection itself was fine.
#: - ``CONNECTION_FAILED``: the connection could not be established or the
#:   transport raised before the command could run (DNS, auth, timeout, ...).
#: - ``NOT_STARTED``: the item was queued but never ran, typically because the
#:   group was cancelled.
SUCCESS = "success"
COMMAND_FAILED = "command_failed"
CONNECTION_FAILED = "connection_failed"
NOT_STARTED = "not_started"


def _classify(exception):
    # Invoke's Failure hierarchy (UnexpectedExit and friends) means the
    # command itself ran but exited poorly; anything else is a transport /
    # connection problem.
    if isinstance(exception, Failure):
        return COMMAND_FAILED
    return CONNECTION_FAILED


class ResultEntry:
    """
    The outcome of a single input item within a `.GroupResult` manifest.

    Attributes:
        index: Position of the item in the original input (0-based).
        connection: The `.Connection` the item ran (or would have run) on.
        status: One of `.SUCCESS`, `.COMMAND_FAILED`, `.CONNECTION_FAILED`,
            or `.NOT_STARTED`.
        result: The return value of the called method, when it succeeded;
            otherwise ``None``.
        exception: The exception raised, when the item failed; otherwise
            ``None``.

    The repr deliberately only shows the index, host and status -- never the
    result/exception bodies -- so that a report never leaks passwords, full
    environments or connection parameters.
    """

    __slots__ = ("index", "connection", "status", "result", "exception")

    def __init__(
        self,
        index,
        connection,
        status=NOT_STARTED,
        result=None,
        exception=None,
    ):
        self.index = index
        self.connection = connection
        self.status = status
        self.result = result
        self.exception = exception

    def __repr__(self):
        host = self.connection.host
        if host is None:
            host = "?"
        return "<ResultEntry index={} host={!r} status={}>".format(
            self.index, host, self.status
        )


class Group(list):
    """
    A collection of `.Connection` objects whose API operates on its contents.

    .. warning::
        **This is a partially abstract class**; you need to use one of its
        concrete subclasses (such as `.SerialGroup` or `.ThreadingGroup`) or
        you'll get ``NotImplementedError`` on most of the methods.

    Most methods in this class wrap those of `.Connection` and will accept the
    same arguments; however their return values and exception-raising behavior
    differ:

    - Return values are dict-like objects (`.GroupResult`) mapping
      `.Connection` objects to the return value for the respective connections:
      `.Group.run` returns a map of `.Connection` to `.runners.Result`,
      `.Group.get` returns a map of `.Connection` to `.transfer.Result`, etc.
    - The `.GroupResult` also exposes an ordered `~.GroupResult.entries`
      manifest of `.ResultEntry` objects, one per input item, in input order.
      Each entry records whether the item succeeded, failed at the command
      level, failed at the connection level, or never started (see
      `.ResultEntry`). The manifest is keyed by input position, so multiple
      aliases pointing at the same address remain distinct entries and the
      order never depends on thread completion order.
    - If any connections encountered exceptions, a `.GroupException` is raised,
      which is a thin wrapper around what would otherwise have been the
      `.GroupResult` returned; within that wrapped `.GroupResult`, the
      excepting connections map to the exception that was raised, in place of a
      ``Result`` (as no ``Result`` was obtained.) Any non-excepting connections
      will have a ``Result`` value, as normal. A partial result -- where some
      items failed or were cancelled before starting -- is never returned as if
      it were fully successful.

    For example, when no exceptions occur, a session might look like this::

        >>> group = SerialGroup('host1', 'host2')
        >>> group.run("this is fine")
        {
            <Connection host='host1'>: <Result cmd='this is fine' exited=0>,
            <Connection host='host2'>: <Result cmd='this is fine' exited=0>,
        }

    With exceptions (anywhere from 1 to "all of them"), it looks like so; note
    the different exception classes, e.g. `~invoke.exceptions.UnexpectedExit`
    for a completed session whose command exited poorly, versus
    `socket.gaierror` for a host that had DNS problems::

        >>> group = SerialGroup('host1', 'host2', 'notahost')
        >>> group.run("will it blend?")
        {
            <Connection host='host1'>: <Result cmd='will it blend?' exited=0>,
            <Connection host='host2'>: <UnexpectedExit: cmd='...' exited=1>,
            <Connection host='notahost'>: gaierror(...),
        }

    As with `.Connection`, `.Group` objects may be used as context managers,
    which will automatically `.close` the object on block exit.

    .. versionadded:: 2.0
    .. versionchanged:: 2.4
        Added context manager behavior.
    .. versionchanged:: 3.0
        Added the ordered `.GroupResult.entries` manifest with per-item
        status (success / command failure / connection failure / not started),
        input-order preservation and safe repr.
    """

    def __init__(self, *hosts, **kwargs):
        """
        Create a group of connections from one or more shorthand host strings.

        See `.Connection` for details on the format of these strings - they
        will be used as the first positional argument of `.Connection`
        constructors.

        Any keyword arguments given will be forwarded directly to those
        `.Connection` constructors as well. For example, to get a serially
        executing group object that connects to ``admin@host1``,
        ``admin@host2`` and ``admin@host3``, and forwards your SSH agent too::

            group = SerialGroup(
                "host1", "host2", "host3", user="admin", forward_agent=True,
            )

        .. versionchanged:: 2.3
            Added ``**kwargs`` (was previously only ``*hosts``).
        """
        # TODO: #563, #388 (could be here or higher up in Program area)
        self.extend([Connection(host, **kwargs) for host in hosts])

    @classmethod
    def from_connections(cls, connections, **kwargs):
        """
        Alternate constructor accepting `.Connection` objects.

        Any keyword arguments are forwarded to the concrete subclass
        constructor (e.g. ``max_workers`` for `.ThreadingGroup`).

        .. versionadded:: 2.0
        .. versionchanged:: 3.0
            Added ``**kwargs`` forwarding.
        """
        # TODO: *args here too; or maybe just fold into __init__ and type
        # check?
        group = cls(**kwargs)
        group.extend(connections)
        return group

    def _do(self, method, *args, **kwargs):
        # TODO: rename this something public & commit to an API for user
        # subclasses
        raise NotImplementedError

    def run(self, *args, **kwargs):
        """
        Executes `.Connection.run` on all member `Connections <.Connection>`.

        :returns: a `.GroupResult`.

        .. versionadded:: 2.0
        """
        # TODO: how to change method of execution across contents? subclass,
        # kwargs, additional methods, inject an executor? Doing subclass for
        # now, but not 100% sure it's the best route.
        # TODO: also need way to deal with duplicate connections (see THOUGHTS)
        return self._do("run", *args, **kwargs)

    def sudo(self, *args, **kwargs):
        """
        Executes `.Connection.sudo` on all member `Connections <.Connection>`.

        :returns: a `.GroupResult`.

        .. versionadded:: 2.6
        """
        # TODO: see run() TODOs
        return self._do("sudo", *args, **kwargs)

    # TODO: this all needs to mesh well with similar strategies applied to
    # entire tasks - so that may still end up factored out into Executors or
    # something lower level than both those and these?

    # TODO: local? Invoke wants ability to do that on its own though, which
    # would be distinct from Group. (May want to switch Group to use that,
    # though, whatever it ends up being? Eg many cases where you do want to do
    # some local thing either N times identically, or parameterized by remote
    # cxn values)

    def put(self, *args, **kwargs):
        """
        Executes `.Connection.put` on all member `Connections <.Connection>`.

        This is a straightforward application: aside from whatever the concrete
        group subclass does for concurrency or lack thereof, the effective
        result is like running a loop over the connections and calling their
        ``put`` method.

        :returns:
            a `.GroupResult` whose values are `.transfer.Result` instances.

        .. versionadded:: 2.6
        """
        return self._do("put", *args, **kwargs)

    def get(self, *args, **kwargs):
        """
        Executes `.Connection.get` on all member `Connections <.Connection>`.

        .. note::
            This method changes some behaviors over e.g. directly calling
            `.Connection.get` on a ``for`` loop of connections; the biggest is
            that the implied default value for the ``local`` parameter is
            ``"{host}/"``, which triggers use of local path parameterization
            based on each connection's target hostname.

            Thus, unless you override ``local`` yourself, a copy of the
            downloaded file will be stored in (relative) directories named
            after each host in the group.

        .. warning::
            Using file-like objects as the ``local`` argument is not currently
            supported, as it would be equivalent to supplying that same object
            to a series of individual ``get()`` calls.

        :returns:
            a `.GroupResult` whose values are `.transfer.Result` instances.

        .. versionadded:: 2.6
        """
        # TODO 4.0: consider making many of these into kwarg-only methods? then
        # below could become kwargs.setdefault() if desired.
        # TODO: do we care enough to handle explicitly given, yet falsey,
        # values? it's a lot more complexity for a corner case.
        if len(args) < 2 and "local" not in kwargs:
            kwargs["local"] = "{host}/"
        return self._do("get", *args, **kwargs)

    def close(self):
        """
        Executes `.Connection.close` on all member `Connections <.Connection>`.

        .. versionadded:: 2.4
        """
        for cxn in self:
            cxn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class SerialGroup(Group):
    """
    Subclass of `.Group` which executes in simple, serial fashion.

    .. versionadded:: 2.0
    """

    def _do(self, method, *args, **kwargs):
        results = GroupResult()
        # Pre-populate the manifest in input order so order never depends on
        # completion timing.
        for index, cxn in enumerate(self):
            results.add(cxn, index=index)
        for index, cxn in enumerate(self):
            try:
                result = getattr(cxn, method)(*args, **kwargs)
                results.set_result(index, result)
            except Exception as e:
                results.set_exception(index, e)
        if not results.complete:
            raise GroupException(results)
        return results


def thread_worker(queue, results, method, args, kwargs, cancel_event, lock):
    """
    Worker body for `.ThreadingGroup`.

    Pulls ``(index, cxn)`` work items from ``queue`` until it is empty or the
    group is cancelled. Each item is executed exactly once; its result or
    exception is recorded against the item's index in ``results``.

    When cancelled, items still queued are drained and marked `.NOT_STARTED`;
    items already running finish normally and keep their real outcome.
    """
    while True:
        with lock:
            if cancel_event.is_set():
                _drain_not_started(queue, results)
                return
            try:
                index, cxn = queue.get(block=False)
            except Empty:
                return
        try:
            result = getattr(cxn, method)(*args, **kwargs)
            with lock:
                results.set_result(index, result)
        except Exception as e:
            with lock:
                results.set_exception(index, e)


def _drain_not_started(queue, results):
    # Caller holds ``lock``; items pulled here never ran, so mark them.
    while True:
        try:
            index, _cxn = queue.get(block=False)
        except Empty:
            return
        results.set_not_started(index)
        queue.task_done()


class ThreadingGroup(Group):
    """
    Subclass of `.Group` which uses threading to execute concurrently.

    Work is handed to a pool of worker threads via a queue, so items not yet
    started when the group is cancelled are recorded as `.NOT_STARTED` instead
    of being silently dropped. Call `.cancel` from another thread to stop
    pulling new items; already-running items still finish and keep their real
    outcome.

    :param max_workers: Number of worker threads. Defaults to the number of
        connections (all items start immediately). Set it smaller than the
        group size to make queued / not-started items observable.

    .. versionadded:: 2.0
    .. versionchanged:: 3.0
        Added the ordered result manifest, `.cancel` support and the
        ``max_workers`` parameter.
    """

    def __init__(self, *hosts, max_workers=None, **kwargs):
        self._max_workers = max_workers
        self._cancel_event = None
        super().__init__(*hosts, **kwargs)

    def cancel(self):
        """
        Cancel the current run.

        Items that have not yet started are marked `.NOT_STARTED`; items
        already running finish normally. Safe to call from another thread
        while the group method is blocking.
        """
        if self._cancel_event is not None:
            self._cancel_event.set()

    def _do(self, method, *args, **kwargs):
        results = GroupResult()
        # Pre-populate the manifest in input order so order never depends on
        # completion timing.
        for index, cxn in enumerate(self):
            results.add(cxn, index=index)

        queue = Queue()
        for index, cxn in enumerate(self):
            queue.put((index, cxn))

        self._cancel_event = threading.Event()
        lock = threading.Lock()
        pool_size = self._max_workers
        if pool_size is None:
            pool_size = len(self)
        pool_size = max(1, pool_size)

        threads = []
        for _ in range(pool_size):
            thread = ExceptionHandlingThread(
                target=thread_worker,
                kwargs=dict(
                    queue=queue,
                    results=results,
                    method=method,
                    args=args,
                    kwargs=kwargs,
                    cancel_event=self._cancel_event,
                    lock=lock,
                ),
            )
            threads.append(thread)
        for thread in threads:
            thread.start()
        for thread in threads:
            # TODO: configurable join timeout
            thread.join()

        # A worker should never raise (it captures per-item exceptions), but
        # if one died unexpectedly the items it had pulled are neither queued
        # nor recorded; they stay NOT_STARTED (the default), which is honest.
        if not results.complete:
            raise GroupException(results)
        return results


class GroupResult(dict):
    """
    Collection of results and/or exceptions arising from `.Group` methods.

    Acts like a dict, but adds a couple convenience methods, to wit:

    - Keys are the individual `.Connection` objects from within the `.Group`.
    - Values are either return values / results from the called method (e.g.
      `.runners.Result` objects), *or* an exception object, if one prevented
      the method from returning.
    - Subclasses `dict`, so has all dict methods.
    - Has `.succeeded` and `.failed` attributes containing sub-dicts limited to
      just those key/value pairs that succeeded or encountered exceptions,
      respectively.

      - Of note, these attributes allow high level logic, e.g. ``if
        mygroup.run('command').failed`` and so forth.

    Beyond the dict view, `.entries` exposes an ordered list of `.ResultEntry`
    objects -- one per input item, in input order, regardless of thread
    completion order. Each entry records its `.ResultEntry.status` (`.SUCCESS`,
    `.COMMAND_FAILED`, `.CONNECTION_FAILED` or `.NOT_STARTED`). The manifest is
    keyed by input position, so multiple aliases pointing at the same address
    remain distinct entries.

    The repr is safe to log: it only shows each item's index, host and status,
    never the result/exception bodies (which may carry passwords, full
    environments or connection parameters).

    .. versionadded:: 2.0
    .. versionchanged:: 3.0
        Added the ordered `.entries` manifest, per-item statuses and safe
        repr.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._entries = []
        # Honour any initial dict data so succeeded/failed stay consistent.
        for key, value in self.items():
            entry = self.add(key)
            if isinstance(value, BaseException):
                entry.status = _classify(value)
                entry.exception = value
            else:
                entry.status = SUCCESS
                entry.result = value

    # -- manifest construction ---------------------------------------------

    def add(self, connection, index=None):
        """
        Append a `.NOT_STARTED` entry for ``connection``.

        Items are added in input order, so the manifest never depends on thread
        completion timing.
        """
        if index is None:
            index = len(self._entries)
        entry = ResultEntry(index=index, connection=connection)
        self._entries.append(entry)
        return entry

    def _entry_for(self, connection):
        # Identity-based: equal-but-distinct Connections (e.g. two aliases for
        # the same address) are separate entries.
        for entry in self._entries:
            if entry.connection is connection:
                return entry
        return None

    def set_result(self, index, result):
        """Record a successful ``result`` for the item at ``index``."""
        entry = self._entries[index]
        entry.status = SUCCESS
        entry.result = result
        entry.exception = None
        super().__setitem__(entry.connection, result)

    def set_exception(self, index, exception):
        """Record a failure (command or connection) for the item."""
        entry = self._entries[index]
        entry.status = _classify(exception)
        entry.exception = exception
        entry.result = None
        super().__setitem__(entry.connection, exception)

    def set_not_started(self, index):
        """Mark the item as never having run (e.g. after cancel)."""
        entry = self._entries[index]
        entry.status = NOT_STARTED
        entry.result = None
        entry.exception = None

    # -- backward-compatible dict interface --------------------------------

    def __setitem__(self, key, value):
        # Allow the legacy ``results[cxn] = value`` pattern; classify by value.
        entry = self._entry_for(key)
        if entry is None:
            entry = self.add(key)
        if isinstance(value, BaseException):
            entry.status = _classify(value)
            entry.exception = value
            entry.result = None
        else:
            entry.status = SUCCESS
            entry.result = value
            entry.exception = None
        super().__setitem__(key, value)

    # -- manifest access ---------------------------------------------------

    @property
    def entries(self):
        """
        The ordered list of `.ResultEntry` objects, one per input item.
        """
        return list(self._entries)

    @property
    def statuses(self):
        """The status of every entry, in input order."""
        return [entry.status for entry in self._entries]

    @property
    def succeeded(self):
        """
        A sub-dict containing only successful results.

        .. versionadded:: 2.0
        """
        out = {}
        for entry in self._entries:
            if entry.status == SUCCESS:
                out[entry.connection] = entry.result
        return out

    @property
    def failed(self):
        """
        A sub-dict containing only failed results (command or connection).

        .. versionadded:: 2.0
        """
        out = {}
        for entry in self._entries:
            if entry.status in (COMMAND_FAILED, CONNECTION_FAILED):
                out[entry.connection] = entry.exception
        return out

    @property
    def not_started(self):
        """Entries that never ran (e.g. after cancellation)."""
        return [
            entry for entry in self._entries if entry.status == NOT_STARTED
        ]

    @property
    def complete(self):
        """
        ``True`` if every item succeeded; ``False`` for any failure or
        not-started item. A partial result must never be reported as fully
        successful.
        """
        return all(entry.status == SUCCESS for entry in self._entries)

    def __repr__(self):
        return "<GroupResult complete={} entries=[{}]>".format(
            self.complete,
            ", ".join(repr(entry) for entry in self._entries),
        )
