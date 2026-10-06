import threading

import pytest

from fabric import Config, Connection
from fabric.testing.base import Session, Command
from fabric.testing.fixtures import remote  # noqa: F401


def _make_cxn():
    return Connection("host", config=Config({"run": {"in_stream": False}}))


class CwdScope:
    class basic:
        def prefixes_command_with_cd(self, remote):
            chan = remote.expect()
            cxn = _make_cxn()
            with cxn.cd("/some/path"):
                cxn.run("ls")
            chan.exec_command.assert_called_once_with("cd /some/path && ls")

        def no_prefix_when_no_scope(self, remote):
            chan = remote.expect()
            cxn = _make_cxn()
            cxn.run("ls")
            chan.exec_command.assert_called_once_with("ls")

        def prefixes_sudo_command(self, remote):
            chan = remote.expect()
            cxn = _make_cxn()
            with cxn.cd("/some/path"):
                cxn.sudo("whoami")
            called_cmd = chan.exec_command.call_args[0][0]
            assert called_cmd.startswith("cd /some/path && ")
            assert "sudo" in called_cmd
            assert "whoami" in called_cmd

    class quoting:
        def quotes_paths_with_spaces(self, remote):
            chan = remote.expect()
            cxn = _make_cxn()
            with cxn.cd("/some path"):
                cxn.run("ls")
            chan.exec_command.assert_called_once_with(
                "cd '/some path' && ls"
            )

        def quotes_paths_with_semicolons(self, remote):
            chan = remote.expect()
            cxn = _make_cxn()
            with cxn.cd("/tmp; rm -rf /"):
                cxn.run("ls")
            chan.exec_command.assert_called_once_with(
                "cd '/tmp; rm -rf /' && ls"
            )

        def quotes_paths_with_single_quotes(self, remote):
            chan = remote.expect()
            cxn = _make_cxn()
            with cxn.cd("/tmp/it's"):
                cxn.run("ls")
            expected = "cd '/tmp/it'\"'\"'s' && ls"
            chan.exec_command.assert_called_once_with(expected)

        def quotes_paths_with_double_quotes(self, remote):
            chan = remote.expect()
            cxn = _make_cxn()
            with cxn.cd('/tmp/foo"bar'):
                cxn.run("ls")
            chan.exec_command.assert_called_once_with(
                'cd \'/tmp/foo"bar\' && ls'
            )

        def quotes_paths_with_dollar_signs(self, remote):
            chan = remote.expect()
            cxn = _make_cxn()
            with cxn.cd("/tmp/$HOME"):
                cxn.run("ls")
            chan.exec_command.assert_called_once_with(
                "cd '/tmp/$HOME' && ls"
            )

    class nesting:
        def joins_relative_paths(self, remote):
            chan = remote.expect()
            cxn = _make_cxn()
            with cxn.cd("/a"):
                with cxn.cd("b"):
                    cxn.run("ls")
            chan.exec_command.assert_called_once_with("cd /a/b && ls")

        def absolute_path_overrides(self, remote):
            chan = remote.expect()
            cxn = _make_cxn()
            with cxn.cd("/a"):
                with cxn.cd("/b"):
                    cxn.run("ls")
            chan.exec_command.assert_called_once_with("cd /b && ls")

        def restores_outer_scope_on_exit(self, remote):
            session = Session(commands=[Command(), Command(), Command()])
            remote.expect_sessions(session)
            cxn = _make_cxn()
            with cxn.cd("/a"):
                cxn.run("ls")
                with cxn.cd("/b"):
                    cxn.run("ls")
                cxn.run("ls")
            calls = [
                c[0][0]
                for ch in session.channels
                for c in ch.exec_command.call_args_list
            ]
            assert calls == [
                "cd /a && ls",
                "cd /b && ls",
                "cd /a && ls",
            ]

        def restores_outer_scope_on_exception(self, remote):
            chan = remote.expect()
            cxn = _make_cxn()
            with cxn.cd("/a"):
                with pytest.raises(RuntimeError):
                    with cxn.cd("/b"):
                        raise RuntimeError("boom")
                cxn.run("ls")
            chan.exec_command.assert_called_once_with("cd /a && ls")

        def restores_to_no_scope_on_outer_exit(self, remote):
            session = Session(commands=[Command(), Command()])
            remote.expect_sessions(session)
            cxn = _make_cxn()
            with cxn.cd("/a"):
                cxn.run("ls")
            cxn.run("ls")
            calls = [
                c[0][0]
                for ch in session.channels
                for c in ch.exec_command.call_args_list
            ]
            assert calls == ["cd /a && ls", "ls"]

    class snapshot:
        def command_is_bound_to_snapshot_at_execution_time(self, remote):
            chan = remote.expect()
            cxn = _make_cxn()
            with cxn.cd("/a"):
                cxn.run("ls")
            chan.exec_command.assert_called_once_with("cd /a && ls")

    class threading:
        def concurrent_threads_are_isolated(self, remote):
            session = Session(commands=[Command(), Command()])
            remote.expect_sessions(session)
            cxn = _make_cxn()

            def worker(path):
                with cxn.cd(path):
                    cxn.run("echo hello")

            t1 = threading.Thread(target=worker, args=("/path one",))
            t2 = threading.Thread(target=worker, args=("/path two",))
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            calls = [
                c[0][0]
                for ch in session.channels
                for c in ch.exec_command.call_args_list
            ]
            assert "cd '/path one' && echo hello" in calls
            assert "cd '/path two' && echo hello" in calls
            assert calls.count("cd '/path one' && echo hello") == 1
            assert calls.count("cd '/path two' && echo hello") == 1

        def exception_in_one_thread_does_not_affect_other(self, remote):
            session = Session(commands=[Command(), Command()])
            remote.expect_sessions(session)
            cxn = _make_cxn()

            def worker_ok(path):
                with cxn.cd(path):
                    cxn.run("echo ok")

            def worker_fail(path):
                try:
                    with cxn.cd(path):
                        cxn.run("echo fail")
                        raise RuntimeError("boom")
                except RuntimeError:
                    pass

            t1 = threading.Thread(target=worker_ok, args=("/ok",))
            t2 = threading.Thread(target=worker_fail, args=("/fail",))
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            calls = [
                c[0][0]
                for ch in session.channels
                for c in ch.exec_command.call_args_list
            ]
            assert "cd /ok && echo ok" in calls
