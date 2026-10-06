"""
Explicit, thread-isolated remote working-directory scope tracking.

Unlike a naïve implementation that mutates a single per-connection ``list``
shared by every caller (which makes two threads using the same
`.Connection` bleed directory state into each other's commands), this module
keeps a *per-thread* stack of scope objects.

Each scope is a tiny immutable-ish record of the path handed to
`.Connection.cd` plus a reference to its parent scope. A "path snapshot" for a
given command invocation is derived up front, at the moment the command is
about to run, by walking the live stack; the resulting chain of ``cd``
arguments is then bound to that single command execution. Nothing reads or
guesses directory state off a mutable shared stack while a command is in
flight, and no remote probing commands (``pwd`` and friends) are ever
emitted -- the directory chain is reconstructed solely from the explicit
scope entries.

All paths interpolated into remote shell command strings are passed through
`shlex.quote`, so values containing spaces, quotes or shell metacharacters
can never be interpreted as additional commands or flags.
"""

import threading
from contextlib import contextmanager
from shlex import quote


def _is_absolute(path):
    # POSIX semantics only: the remote end is a Unix shell, regardless of the
    # local OS, so we deliberately do not use os.path here.
    return path.startswith("/") or path.startswith("~")


class RemoteCwdScope:
    """
    A single entry in a thread's remote ``cd`` scope chain.

    Each scope records the ``path`` it was entered with and the ``parent``
    scope (if any) that was active at entry time. Scopes never mutate after
    construction, which makes the per-command snapshot trivially safe even if a
    sibling thread enters or exits its own scopes concurrently.

    .. versionadded:: 3.1
    """

    def __init__(self, path, parent=None):
        #: The raw, caller-supplied path for this scope (already stringified).
        self.path = path
        #: The enclosing `.RemoteCwdScope`, or ``None`` at the outermost level.
        self.parent = parent

    def _chain(self):
        scopes = []
        scope = self
        while scope is not None:
            scopes.append(scope)
            scope = scope.parent
        scopes.reverse()
        return scopes

    def segments(self):
        """
        Return the effective relative ``cd`` segments for this scope chain.

        An absolute (or ``~``-relative) path resets the chain -- matching the
        behavior of nesting shell ``cd`` commands, where an absolute inner
        ``cd`` discards the outer directory. All earlier segments are dropped
        in that case.

        .. versionadded:: 3.1
        """
        segments = [scope.path for scope in self._chain()]
        for index, segment in enumerate(segments):
            if _is_absolute(segment):
                segments = segments[index:]
        return segments

    def cd_arguments(self):
        """
        Return the shell-safe ``cd`` arguments for this scope chain.

        Each path segment is individually quoted with `shlex.quote`, so paths
        containing spaces (e.g. ``/srv/my app``) or quote characters
        (e.g. ``it's"weird``) cannot break out of the ``cd`` argument.

        .. versionadded:: 3.1
        """
        return [quote(segment) for segment in self.segments()]

    def prefix_command(self):
        """
        Render the full nested ``cd`` prefix.

        E.g. ``cd '/srv/my app' && cd htdocs``.

        Returns the empty string when there are no active scopes, so callers
        can unconditionally prepend it to a user command.

        .. versionadded:: 3.1
        """
        arguments = self.cd_arguments()
        if not arguments:
            return ""
        return " && ".join("cd {}".format(arg) for arg in arguments)


class RemoteCwdStacks:
    """
    Per-thread owner of remote cwd scope stacks, scoped to one `.Connection`.

    A fresh instance is created per `.Connection`; isolation *between threads*
    is provided by an internal `threading.local`, so two worker threads sharing
    a connection may each open their own ``with cxn.cd(...)`` blocks without
    ever observing each other's directory scope.

    .. versionadded:: 3.1
    """

    def __init__(self):
        self._local = threading.local()

    def _stack(self):
        stack = getattr(self._local, "stack", None)
        if stack is None:
            stack = []
            self._local.stack = stack
        return stack

    def current(self):
        """
        Return the innermost active `.RemoteCwdScope` for this thread, or None.

        .. versionadded:: 3.1
        """
        stack = self._stack()
        return stack[-1] if stack else None

    def push(self, path):
        """
        Enter a new scope for ``path``; returns the new `.RemoteCwdScope`.

        ``path`` is stringified eagerly so later mutation of a passed-in
        path-like object cannot change the bound scope.

        .. versionadded:: 3.1
        """
        path = str(path)
        scope = RemoteCwdScope(path=path, parent=self.current())
        self._stack().append(scope)
        return scope

    def pop(self, scope):
        """
        Exit ``scope``, restoring the outer scope for this thread.

        A defensive check ensures we cannot pop a different thread's frame or
        an already-exited one; normal nesting guarantees the top of the stack
        is the scope being left.

        .. versionadded:: 3.1
        """
        stack = self._stack()
        if not stack or stack[-1] is not scope:
            raise RuntimeError("cd scope exited in an unexpected order!")
        stack.pop()

    @contextmanager
    def scope(self, path):
        """
        Context manager pushing ``path`` and always popping it on exit.

        The pop runs in a ``finally`` block, so the outer directory scope is
        restored even when the wrapped block raises.

        .. versionadded:: 3.1
        """
        scope = self.push(path)
        try:
            yield scope
        finally:
            self.pop(scope)

    def snapshot(self):
        """
        Bind this invocation to an immutable copy of the active cd segments.

        Returns a tuple of *raw* (unquoted) path segments captured from the
        current thread's chain. The tuple is independent of the live stack, so
        entering or exiting scopes afterwards (including from other threads)
        cannot alter what the command executes with.

        .. versionadded:: 3.1
        """
        current = self.current()
        if current is None:
            return ()
        return tuple(current.segments())

    def wrap(self, command):
        """
        Prefix ``command`` with the snapshot's shell-safe ``cd`` chain.

        The snapshot is taken at call time and rendered from the captured
        scope chain rather than from any later-mutated stack data.
        """
        current = self.current()
        if current is None:
            return command
        prefix = current.prefix_command()
        return "{} && {}".format(prefix, command)
