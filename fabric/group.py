from queue import Queue

from invoke.exceptions import UnexpectedExit
from invoke.util import ExceptionHandlingThread

from .connection import Connection
from .exceptions import GroupException


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
    - If any connections encountered exceptions, a `.GroupException` is raised,
      which is a thin wrapper around what would otherwise have been the
      `.GroupResult` returned; within that wrapped `.GroupResult`, the
      excepting connections map to the exception that was raised, in place of a
      ``Result`` (as no ``Result`` was obtained.) Any non-excepting connections
      will have a ``Result`` value, as normal.

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
    def from_connections(cls, connections):
        """
        Alternate constructor accepting `.Connection` objects.

        .. versionadded:: 2.0
        """
        # TODO: *args here too; or maybe just fold into __init__ and type
        # check?
        group = cls()
        group.extend(connections)
        return group

    def _do(self, method, *args, **kwargs):
        # TODO: rename this something public & commit to an API for user
        # subclasses
        raise NotImplementedError

    def _do_manifest(self, *args, **kwargs):
        raise NotImplementedError

    def run_manifest(self, *args, **kwargs):
        """
        Like `.run`, but returns an explicit, ordered `.GroupManifest`.

        Where `.run` collapses outcomes into a `.GroupResult` (and raises
        `.GroupException` on any failure), this method *always* returns a
        `.GroupManifest`: a list of `.HostResult` entries, one per member
        connection, in the same order the connections were given to the
        group. Each entry records exactly one of four statuses:

        - ``success``: the command ran and exited cleanly;
        - ``command_failed``: the command ran but exited poorly (e.g. an
          `~invoke.exceptions.UnexpectedExit` was raised);
        - ``connection_failed``: the command never ran because obtaining
          the connection / session failed (any other exception);
        - ``not_started``: the connection was never attempted, e.g. because
          cancellation was requested before its turn came.

        Because entries are keyed by input position, two host strings that
        resolve to the same address (aliases, or literal duplicates) still
        produce two distinct entries, and entry order never depends on the
        order in which concurrent workers happen to finish.

        :param cancel_event:
            Optional ``threading.Event``-like object. When given, any
            connection whose turn has not yet begun once the event is set
            is skipped and reported as ``not_started``; connections already
            in flight are allowed to finish normally. Cancellation never
            interrupts running items, it only prevents not-yet-started
            ones.

        :returns:
            a `.GroupManifest`. It is never raised as an exception, and a
            partially successful run is visible via ``manifest.ok`` being
            ``False`` (plus the ``.succeeded`` / ``.failed`` /
            ``.not_started`` views) - partial results are never presented
            as total success.

        .. note::
            Manifest entries and their string representations are
            deliberately sanitized: they expose only the input position,
            the host identity string, the status, and (on success) the exit
            code. Passwords, full environment, and connection parameters
            are never included in the report.

        .. versionadded:: 4.0
        """
        return self._do_manifest(*args, **kwargs)

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
        excepted = False
        for cxn in self:
            try:
                results[cxn] = getattr(cxn, method)(*args, **kwargs)
            except Exception as e:
                results[cxn] = e
                excepted = True
        if excepted:
            raise GroupException(results)
        return results

    def _do_manifest(self, *args, **kwargs):
        cancel_event = kwargs.pop("cancel_event", None)
        manifest = GroupManifest(
            HostResult(index=index, host=cxn.host)
            for index, cxn in enumerate(self)
        )
        cancelled = False
        for index, cxn in enumerate(self):
            if cancelled or (
                cancel_event is not None and cancel_event.is_set()
            ):
                # Cancellation only blocks not-yet-started items; mark the
                # rest not_started (their initial status) and stop.
                cancelled = True
                continue
            entry = manifest[index]
            try:
                entry.result = cxn.run(*args, **kwargs)
            except Exception as e:
                entry.exception = e
                entry.status = _classify_exception(e)
            else:
                entry.status = HostResult.SUCCESS
        return manifest


def thread_worker(cxn, queue, method, args, kwargs):
    result = getattr(cxn, method)(*args, **kwargs)
    # TODO: namedtuple or attrs object?
    queue.put((cxn, result))


def manifest_worker(manifest, index, cxn, args, kwargs):
    result = cxn.run(*args, **kwargs)
    entry = manifest[index]
    entry.result = result
    entry.status = HostResult.SUCCESS


class ThreadingGroup(Group):
    """
    Subclass of `.Group` which uses threading to execute concurrently.

    .. versionadded:: 2.0
    """

    def _do(self, method, *args, **kwargs):
        results = GroupResult()
        queue = Queue()
        threads = []
        for cxn in self:
            thread = ExceptionHandlingThread(
                target=thread_worker,
                kwargs=dict(
                    cxn=cxn,
                    queue=queue,
                    method=method,
                    args=args,
                    kwargs=kwargs,
                ),
            )
            threads.append(thread)
        for thread in threads:
            thread.start()
        for thread in threads:
            # TODO: configurable join timeout
            thread.join()
        # Get non-exception results from queue
        while not queue.empty():
            # TODO: io-sleep? shouldn't matter if all threads are now joined
            cxn, result = queue.get(block=False)
            # TODO: outstanding musings about how exactly aggregate results
            # ought to ideally operate...heterogenous obj like this, multiple
            # objs, ??
            results[cxn] = result
        # Get exceptions from the threads themselves.
        # TODO: in a non-thread setup, this would differ, e.g.:
        # - a queue if using multiprocessing
        # - some other state-passing mechanism if using e.g. coroutines
        # - ???
        excepted = False
        for thread in threads:
            wrapper = thread.exception()
            if wrapper is not None:
                # Outer kwargs is Thread instantiation kwargs, inner is kwargs
                # passed to thread target/body.
                cxn = wrapper.kwargs["kwargs"]["cxn"]
                results[cxn] = wrapper.value
                excepted = True
        if excepted:
            raise GroupException(results)
        return results

    def _do_manifest(self, *args, **kwargs):
        cancel_event = kwargs.pop("cancel_event", None)
        # Pre-populate one entry per input connection, in input order, so
        # result positions never depend on thread completion order, and so
        # duplicate/aliased hosts remain distinct entries.
        manifest = GroupManifest(
            HostResult(index=index, host=cxn.host)
            for index, cxn in enumerate(self)
        )
        threads = []
        for index, cxn in enumerate(self):
            if cancel_event is not None and cancel_event.is_set():
                # Cancellation only blocks not-yet-started items; leave
                # this (and any later) entries at their initial
                # not_started status.
                break
            thread = ExceptionHandlingThread(
                target=manifest_worker,
                kwargs=dict(
                    manifest=manifest,
                    index=index,
                    cxn=cxn,
                    args=args,
                    kwargs=kwargs,
                ),
            )
            threads.append(thread)
        for thread in threads:
            thread.start()
        for thread in threads:
            # TODO: configurable join timeout
            thread.join()
        # Collect exceptions from the threads themselves; already-started
        # threads always run to completion, even if cancellation was
        # requested while they were in flight.
        for thread in threads:
            wrapper = thread.exception()
            if wrapper is not None:
                # Outer kwargs is Thread instantiation kwargs, inner is
                # kwargs passed to thread target/body.
                index = wrapper.kwargs["kwargs"]["index"]
                entry = manifest[index]
                entry.exception = wrapper.value
                entry.status = _classify_exception(wrapper.value)
        return manifest


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

    .. versionadded:: 2.0
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._successes = {}
        self._failures = {}

    def _bifurcate(self):
        # Short-circuit to avoid reprocessing every access.
        if self._successes or self._failures:
            return
        # TODO: if we ever expect .succeeded/.failed to be useful before a
        # GroupResult is fully initialized, this needs to become smarter.
        for key, value in self.items():
            if isinstance(value, BaseException):
                self._failures[key] = value
            else:
                self._successes[key] = value

    @property
    def succeeded(self):
        """
        A sub-dict containing only successful results.

        .. versionadded:: 2.0
        """
        self._bifurcate()
        return self._successes

    @property
    def failed(self):
        """
        A sub-dict containing only failed results.

        .. versionadded:: 2.0
        """
        self._bifurcate()
        return self._failures


def _classify_exception(exception):
    """
    Classify an exception raised by ``Connection.run`` for a `.HostResult`.

    `~invoke.exceptions.UnexpectedExit` means the command executed but
    exited poorly (``command_failed``); anything else prevented the command
    from running at all (``connection_failed``).
    """
    if isinstance(exception, UnexpectedExit):
        return HostResult.COMMAND_FAILED
    return HostResult.CONNECTION_FAILED


class HostResult:
    """
    A single per-host entry within a `.GroupManifest`.

    Attributes:

    - ``index``: position of the connection within the originating
      `.Group`, i.e. its input identity. Two connections that are aliases
      for the same address still get distinct indexes and thus distinct
      ``HostResult`` entries.
    - ``host``: the connection's host identity string (only this string is
      kept - not the `.Connection` itself - so reports can never leak
      passwords, environment, or connection parameters).
    - ``status``: one of ``success``, ``command_failed``,
      ``connection_failed`` or ``not_started`` (see the class constants).
    - ``result``: the `.runners.Result` when ``status == SUCCESS``, else
      ``None``.
    - ``exception``: the exception object when the status is one of the
      failure modes, else ``None``.

    .. versionadded:: 4.0
    """

    #: The command ran and exited cleanly.
    SUCCESS = "success"
    #: The command ran but exited poorly.
    COMMAND_FAILED = "command_failed"
    #: The connection/session could not be obtained; command never ran.
    CONNECTION_FAILED = "connection_failed"
    #: The connection was never attempted (e.g. cancelled before its turn).
    NOT_STARTED = "not_started"

    def __init__(
        self, index, host, status=NOT_STARTED, result=None, exception=None
    ):
        self.index = index
        self.host = host
        self.status = status
        self.result = result
        self.exception = exception

    @property
    def ok(self):
        """
        ``True`` only when this entry's status is ``success``.
        """
        return self.status == self.SUCCESS

    def __repr__(self):
        # Deliberately sanitized: only input position, host identity and
        # status (plus exit code on success). Never the Connection object,
        # its password/environment/connect_kwargs, or result payloads.
        parts = "index={!r} host={!r} status={!r}".format(
            self.index, self.host, self.status
        )
        if self.status == self.SUCCESS and self.result is not None:
            exited = getattr(self.result, "exited", None)
            if exited is not None:
                parts += " exited={!r}".format(exited)
        return "<{} {}>".format(type(self).__name__, parts)


class GroupManifest(list):
    """
    Ordered, explicit per-host result manifest returned by
    `.Group.run_manifest`.

    A list of `.HostResult` entries, one per connection in the originating
    `.Group`, **in input order** - never in thread-completion order. Unlike
    `.GroupResult` (a dict keyed by `.Connection`), input identity is
    positional, so multiple aliases for the same address remain distinct
    entries.

    Convenience views:

    - ``.succeeded``: entries whose status is ``success``;
    - ``.failed``: entries whose status is ``command_failed`` or
      ``connection_failed``;
    - ``.not_started``: entries never attempted (e.g. due to
      cancellation);
    - ``.ok``: ``True`` only when *every* entry succeeded - a partially
      successful run can never masquerade as total success.

    ``repr`` of this object (and of its entries, and the string returned by
    `.report`) is sanitized: it contains only input positions, host
    identity strings, statuses and exit codes - never passwords, the full
    environment, or connection parameters.

    .. versionadded:: 4.0
    """

    def _by_status(self, *statuses):
        return GroupManifest(x for x in self if x.status in statuses)

    @property
    def succeeded(self):
        """
        Sub-list of entries whose status is ``success``.

        .. versionadded:: 4.0
        """
        return self._by_status(HostResult.SUCCESS)

    @property
    def failed(self):
        """
        Sub-list of entries which ran (or tried to) and failed.

        .. versionadded:: 4.0
        """
        return self._by_status(
            HostResult.COMMAND_FAILED, HostResult.CONNECTION_FAILED
        )

    @property
    def not_started(self):
        """
        Sub-list of entries which were never attempted.

        .. versionadded:: 4.0
        """
        return self._by_status(HostResult.NOT_STARTED)

    @property
    def ok(self):
        """
        ``True`` only when every entry succeeded; never ``True`` for a
        partial or empty-of-success result set.

        .. versionadded:: 4.0
        """
        return bool(self) and all(entry.ok for entry in self)

    def report(self):
        """
        Return a sanitized, human-readable multi-line summary string.

        One line per entry (input position, host identity, status, exit
        code when applicable); contains no passwords, environment, or
        connection parameters.

        .. versionadded:: 4.0
        """
        lines = [
            "GroupManifest: ok={} ({} succeeded, {} failed, "
            "{} not started, {} total)".format(
                self.ok,
                len(self.succeeded),
                len(self.failed),
                len(self.not_started),
                len(self),
            )
        ]
        for entry in self:
            line = "  #{} {}: {}".format(
                entry.index, entry.host, entry.status
            )
            if entry.status == HostResult.SUCCESS and entry.result is not None:
                exited = getattr(entry.result, "exited", None)
                if exited is not None:
                    line += " (exited={})".format(exited)
            lines.append(line)
        return "\n".join(lines)

    def __repr__(self):
        return "<{} ok={} succeeded={} failed={} not_started={}>".format(
            type(self).__name__,
            self.ok,
            len(self.succeeded),
            len(self.failed),
            len(self.not_started),
        )
