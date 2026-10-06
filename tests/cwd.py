"""
Acceptance tests for explicit, thread-isolated remote cwd scope tracking.

Run with::

    pytest tests/cwd.py -v

No SSH, no Docker: a fake (mocked) remote runner records the exact command
string handed to each remote execution, so assertions cover precisely what
would be sent over the wire.

The two headline "scope" examples demonstrated here are:

1. **Nested scopes** -- an outer ``cd`` plus an inner relative ``cd`` compose
   into one shell-quoted chain, and exiting (even via an exception) restores
   the outer path.
2. **Two concurrent thread scopes** -- two worker threads sharing one
   `.Connection` each run inside a directory *containing spaces*; neither
   command ever carries the other thread's cwd.
"""

import shlex
import threading
from unittest.mock import patch

from invoke.runners import Result

from fabric import Connection


# Remote is woven in as a config default, so it must be patched there (the
# same spot the rest of the connection test-suite patches).
remote_path = "fabric.config.Remote"


def _connection_with_fake_runner(Remote, executed):
    """
    Build a Connection whose remote runner is a fake recording runner.

    The fake's ``run`` records ``(thread_ident, command_string)`` for every
    invocation and returns a successful `~invoke.runners.Result`. It never
    opens a socket or runs a subprocess.
    """
    remote = Remote.return_value

    def fake_run(command, **kwargs):
        executed.append((threading.get_ident(), command))
        return Result(command=command, exited=0)

    remote.run.side_effect = fake_run
    # @opens on run()/sudo() would otherwise trigger a real paramiko connect.
    cxn = Connection("host")
    cxn.open = lambda: None
    return cxn


class remote_cd_scopes:
    @patch(remote_path)
    def commands_without_scope_are_not_prefixed(self, Remote):
        executed = []
        cxn = _connection_with_fake_runner(Remote, executed)
        cxn.run("whoami")
        assert executed == [(threading.get_ident(), "whoami")]

    @patch(remote_path)
    def single_scope_prefixes_explicit_cd_and_restores_on_exit(
        self, Remote
    ):
        executed = []
        cxn = _connection_with_fake_runner(Remote, executed)
        with cxn.cd("/srv/app"):
            assert cxn.remote_cwd == "/srv/app"
            cxn.run("ls")
        assert executed[-1][1] == "cd /srv/app && ls"
        # Outer path (none) restored after the scope exits.
        assert cxn.remote_cwd == ""
        cxn.run("whoami")
        assert executed[-1][1] == "whoami"

    @patch(remote_path)
    def nested_scopes_compose_and_unwind_correctly(self, Remote):
        """
        Scope example #1: nested enter/exit, inner dir contains a space.
        """
        executed = []
        cxn = _connection_with_fake_runner(Remote, executed)
        with cxn.cd("/srv"):
            cxn.run("ls")
            assert cxn.remote_cwd == "/srv"
            with cxn.cd("my app"):
                assert cxn.remote_cwd == "/srv/my app"
                cxn.run("pwd")
            # Back to the outer directory after the inner scope exits.
            assert cxn.remote_cwd == "/srv"
            cxn.run("ls")
        assert cxn.remote_cwd == ""
        assert [cmd for _, cmd in executed] == [
            "cd /srv && ls",
            "cd /srv && cd 'my app' && pwd",
            "cd /srv && ls",
        ]

    @patch(remote_path)
    def absolute_inner_path_resets_the_chain(self, Remote):
        executed = []
        cxn = _connection_with_fake_runner(Remote, executed)
        with cxn.cd("/srv"):
            with cxn.cd("/var/log"):
                cxn.run("ls")
            # Absolute inner cd replaces (rather than extends) the chain, so
            # after exiting it the outer scope is active once more.
            cxn.run("ls")
        assert [cmd for _, cmd in executed] == [
            "cd /var/log && ls",
            "cd /srv && ls",
        ]

    @patch(remote_path)
    def scope_is_restored_after_exception_inside_block(self, Remote):
        executed = []
        cxn = _connection_with_fake_runner(Remote, executed)
        with cxn.cd("/outer"):
            try:
                with cxn.cd("inner"):
                    cxn.run("boom")
                    raise RuntimeError("kaboom")
            except RuntimeError:
                pass
            # Exception unwound the inner scope; the outer path is active.
            assert cxn.remote_cwd == "/outer"
            cxn.run("ls")
        assert cxn.remote_cwd == ""
        assert [cmd for _, cmd in executed] == [
            "cd /outer && cd inner && boom",
            "cd /outer && ls",
        ]

    @patch(remote_path)
    def quotes_and_metacharacters_cannot_become_extra_commands(
        self, Remote
    ):
        executed = []
        cxn = _connection_with_fake_runner(Remote, executed)
        evil = '/tmp/it\'s "fun"; touch /tmp/pwned #'
        with cxn.cd(evil):
            cxn.run("ls")
        command = executed[-1][1]
        # Exactly one " && " separates the cd chain from the user command.
        assert command.count(" && ") == 1
        chain_part, user_part = command.split(" && ", 1)
        assert user_part == "ls"
        # shlex must parse the cd argument back into the exact original path:
        # the quote/metacharacters stay inert data, never extra commands.
        assert shlex.split(chain_part) == ["cd", evil]

    @patch(remote_path)
    def no_probing_commands_are_ever_executed(self, Remote):
        executed = []
        cxn = _connection_with_fake_runner(Remote, executed)
        remote = Remote.return_value
        with cxn.cd("/srv/my app"):
            cxn.run("ls")
        # Exactly one remote execution: the cd-prefixed user command itself.
        assert remote.run.call_count == 1
        command = remote.run.call_args[0][0]
        assert command == "cd '/srv/my app' && ls"
        for probe in ("pwd", "dirs", "$PWD"):
            assert probe not in command

    @patch(remote_path)
    def each_invocation_binds_its_own_path_snapshot(self, Remote):
        executed = []
        cxn = _connection_with_fake_runner(Remote, executed)
        with cxn.cd("/outer"):
            with cxn.cd("inner"):
                # Snapshot captured while the inner scope is live...
                snapshot = cxn._remote_cwds.snapshot()
                cxn.run("a")
            # ...remains valid/immutable after that scope has exited.
            cxn.run("b")
        assert snapshot == ("/outer", "inner")
        assert [cmd for _, cmd in executed] == [
            "cd /outer && cd inner && a",
            "cd /outer && b",
        ]

    @patch(remote_path)
    def sudo_is_scoped_and_quoted_too(self, Remote):
        remote = Remote.return_value
        remote.run.side_effect = lambda command, **kwargs: Result(
            command=command, exited=0
        )
        cxn = Connection("host")
        cxn.open = lambda: None
        with cxn.cd("/srv/my app"):
            cxn.sudo("systemctl reload nginx")
        command = remote.run.call_args[0][0]
        # Mirrors upstream invoke cd+sudo composition, but with the directory
        # explicitly shell-quoted so spaces stay inside the cd argument.
        assert command == (
            "sudo -S -p '[sudo] password: ' "
            "cd '/srv/my app' && systemctl reload nginx"
        )
        assert shlex.split(command)[-1] == "nginx"

    @patch(remote_path)
    def two_threads_get_isolated_scopes_with_space_directories(
        self, Remote
    ):
        """
        Scope example #2: concurrent threads, dirs containing spaces.

        Both threads share the *same* Connection (and thus the same fake
        runner). A barrier holds them simultaneously inside their respective
        scopes while their commands are built, proving the command strings
        come from per-thread snapshots rather than a shared mutable cwd stack.
        """
        executed = []
        cxn = _connection_with_fake_runner(Remote, executed)
        barrier = threading.Barrier(2)
        paths = {"one": "/srv/dir one", "two": "/srv/dir two"}
        results = {}

        def worker(label):
            with cxn.cd(paths[label]):
                # Block until both threads are inside their own scope at once.
                barrier.wait()
                cxn.run("build-{}".format(label))
                results[label] = cxn.remote_cwd

        threads = [
            threading.Thread(target=worker, args=("one",)),
            threading.Thread(target=worker, args=("two",)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert results == {
            "one": "/srv/dir one",
            "two": "/srv/dir two",
        }
        by_thread = {}
        for ident, command in executed:
            by_thread.setdefault(ident, []).append(command)
        assert len(by_thread) == 2
        # Match threads to their scopes by command content (dict ordering is
        # incidental here); each recorded command must be exactly one of the
        # two expected strings.
        all_commands = sorted(
            command for thread_cmds in by_thread.values()
            for command in thread_cmds
        )
        assert all_commands == sorted(
            [
                "cd '/srv/dir one' && build-one",
                "cd '/srv/dir two' && build-two",
            ]
        )
        for thread_commands in by_thread.values():
            assert len(thread_commands) == 1
            command = thread_commands[0]
            assert not (
                "dir one" in command and "dir two" in command
            )
        # The main thread's scope was never touched by either worker.
        assert cxn.remote_cwd == ""
        # The fake runner saw exactly the two expected executions.
        assert Remote.return_value.run.call_count == 2
