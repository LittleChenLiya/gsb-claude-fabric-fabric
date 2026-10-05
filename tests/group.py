import socket
import threading
import time
from unittest.mock import Mock
from pytest import mark, raises

from fabric import Connection, Group, SerialGroup, ThreadingGroup, GroupResult
from fabric.group import _classify
from fabric.exceptions import GroupException

RUNNER_METHODS = ("run", "sudo")
TRANSFER_METHODS = ("put", "get")
ALL_METHODS = RUNNER_METHODS + TRANSFER_METHODS
runner_args = ("command",)
runner_kwargs = dict(hide=True, warn=True)
transfer_args = tuple()
transfer_kwargs = dict(local="yokel", remote="goat")
ARGS_BY_METHOD = dict(
    run=runner_args, sudo=runner_args, put=transfer_args, get=transfer_args
)
KWARGS_BY_METHOD = dict(
    run=runner_kwargs,
    sudo=runner_kwargs,
    put=transfer_kwargs,
    get=transfer_kwargs,
)


class Group_:
    class init:
        "__init__"

        def may_be_empty(self):
            assert len(Group()) == 0

        def takes_splat_arg_of_host_strings(self):
            g = Group("foo", "bar")
            assert g[0].host == "foo"
            assert g[1].host == "bar"

        def takes_splat_kwargs_and_passes_them_to_Connections(self):
            g = Group("foo", "bar", user="admin", forward_agent=True)
            assert g[0].host == "foo"
            assert g[0].user == "admin"
            assert g[0].forward_agent is True
            assert g[1].host == "bar"
            assert g[1].user == "admin"
            assert g[1].forward_agent is True

    class from_connections:
        def inits_from_iterable_of_Connections(self):
            g = Group.from_connections((Connection("foo"), Connection("bar")))
            assert len(g) == 2
            assert g[1].host == "bar"

    def acts_like_an_iterable_of_Connections(self):
        g = Group("foo", "bar", "biz")
        assert g[0].host == "foo"
        assert g[-1].host == "biz"
        assert len(g) == 3
        for c in g:
            assert isinstance(c, Connection)

    @mark.parametrize("method", ALL_METHODS)
    def abstract_methods_not_implemented(self, method):
        group = Group()
        with raises(NotImplementedError):
            getattr(group, method)()

    class close_and_contextmanager_behavior:
        def close_closes_all_member_connections(self):
            cxns = [Mock(name=x) for x in ("foo", "bar", "biz")]
            g = Group.from_connections(cxns)
            g.close()
            for c in cxns:
                c.close.assert_called_once_with()

        def contextmanager_behavior_works_like_Connection(self):
            cxns = [Mock(name=x) for x in ("foo", "bar", "biz")]
            g = Group.from_connections(cxns)
            with g as my_g:
                assert my_g is g
            for c in cxns:
                c.close.assert_called_once_with()

    class get:
        class local_defaults_to_host_interpolated_path:
            def when_no_arg_or_kwarg_given(self):
                g = Group("host1", "host2")
                g._do = Mock()
                g.get(remote="whatever")
                g._do.assert_called_with(
                    "get", remote="whatever", local="{host}/"
                )

            def not_when_arg_given(self):
                g = Group("host1", "host2")
                g._do = Mock()
                g.get("whatever", "lol")
                # No local kwarg passed.
                g._do.assert_called_with("get", "whatever", "lol")

            def not_when_kwarg_given(self):
                g = Group("host1", "host2")
                g._do = Mock()
                g.get(remote="whatever", local="lol")
                # Doesn't stomp given local arg
                g._do.assert_called_with("get", remote="whatever", local="lol")


def _make_serial_tester(method, cxns, index, args, kwargs):
    args = args[:]
    kwargs = kwargs.copy()

    def tester(*a, **k):  # Don't care about doing anything with our own args.
        car, cdr = index, index + 1
        predecessors = cxns[:car]
        successors = cxns[cdr:]
        for predecessor in predecessors:
            getattr(predecessor, method).assert_called_with(*args, **kwargs)
        for successor in successors:
            assert not getattr(successor, method).called

    return tester


class SerialGroup_:
    @mark.parametrize("method", ALL_METHODS)
    def executes_arguments_on_contents_run_serially(self, method):
        "executes arguments on contents' run() serially"
        cxns = [Connection(x) for x in ("host1", "host2", "host3")]
        args = ARGS_BY_METHOD[method]
        kwargs = KWARGS_BY_METHOD[method]
        for index, cxn in enumerate(cxns):
            side_effect = _make_serial_tester(
                method, cxns, index, args, kwargs
            )
            setattr(cxn, method, Mock(side_effect=side_effect))
        g = SerialGroup.from_connections(cxns)
        getattr(g, method)(*args, **kwargs)
        # Sanity check, e.g. in case none of them were actually run
        for cxn in cxns:
            getattr(cxn, method).assert_called_with(*args, **kwargs)

    @mark.parametrize("method", ALL_METHODS)
    def errors_in_execution_capture_and_continue_til_end(self, method):
        cxns = [Mock(name=x) for x in ("host1", "host2", "host3")]

        class OhNoz(Exception):
            pass

        onoz = OhNoz()
        getattr(cxns[1], method).side_effect = onoz
        g = SerialGroup.from_connections(cxns)
        try:
            getattr(g, method)("whatever", hide=True)
        except GroupException as e:
            result = e.result
        else:
            assert False, "Did not raise GroupException!"
        succeeded = {
            cxns[0]: getattr(cxns[0], method).return_value,
            cxns[2]: getattr(cxns[2], method).return_value,
        }
        failed = {cxns[1]: onoz}
        expected = succeeded.copy()
        expected.update(failed)
        assert result == expected
        assert result.succeeded == succeeded
        assert result.failed == failed

    @mark.parametrize("method", ALL_METHODS)
    def returns_results_mapping(self, method):
        cxns = [Mock(name=x) for x in ("host1", "host2", "host3")]
        g = SerialGroup.from_connections(cxns)
        result = getattr(g, method)("whatever", hide=True)
        assert isinstance(result, GroupResult)
        expected = {x: getattr(x, method).return_value for x in cxns}
        assert result == expected
        assert result.succeeded == expected
        assert result.failed == {}


class ThreadingGroup_:
    @mark.parametrize("method", ALL_METHODS)
    def executes_arguments_on_contents_run_via_threading(self, method):
        # Use Mock connections so no real network I/O occurs.
        cxns = [Mock(host=x) for x in ("host1", "host2", "host3")]
        g = ThreadingGroup.from_connections(cxns)
        args = ARGS_BY_METHOD[method]
        kwargs = KWARGS_BY_METHOD[method]
        results = getattr(g, method)(*args, **kwargs)
        expected = {x: getattr(x, method).return_value for x in cxns}
        assert results == expected
        # The ordered manifest has one entry per input item, in input order,
        # regardless of thread completion timing.
        assert [entry.connection for entry in results.entries] == cxns
        assert results.statuses == ["success"] * len(cxns)
        assert results.succeeded == expected
        assert results.failed == {}

    @mark.parametrize("method", ALL_METHODS)
    def work_items_enqueued_in_input_order(self, method):
        # Work items are enqueued as (index, cxn) in input order, and the
        # manifest records results back in input order regardless of thread
        # completion timing.
        cxns = [Mock(host=x) for x in ("host1", "host2", "host3")]
        g = ThreadingGroup.from_connections(cxns)
        results = getattr(g, method)(
            *ARGS_BY_METHOD[method], **KWARGS_BY_METHOD[method]
        )
        expected = {x: getattr(x, method).return_value for x in cxns}
        assert results == expected
        # The ordered manifest has one entry per input item, in input order.
        assert [entry.connection for entry in results.entries] == cxns
        assert results.statuses == ["success", "success", "success"]
        assert results.succeeded == expected
        assert results.failed == {}

    @mark.parametrize("method", ALL_METHODS)
    def bubbles_up_errors_within_threads(self, method):
        # TODO: I feel like this is the first spot where a raw
        # ThreadException might need tweaks, at least presentation-wise,
        # since we're no longer dealing with truly background threads (IO
        # workers and tunnels), but "middle-ground" threads the user is
        # kind of expecting (and which they might expect to encounter
        # failures).
        cxns = [Mock(host=x) for x in ("host1", "host2", "host3")]

        class OhNoz(Exception):
            pass

        onoz = OhNoz()
        getattr(cxns[1], method).side_effect = onoz
        g = ThreadingGroup.from_connections(cxns)
        try:
            getattr(g, method)(
                *ARGS_BY_METHOD[method], **KWARGS_BY_METHOD[method]
            )
        except GroupException as e:
            result = e.result
        else:
            assert False, "Did not raise GroupException!"
        succeeded = {
            cxns[0]: getattr(cxns[0], method).return_value,
            cxns[2]: getattr(cxns[2], method).return_value,
        }
        failed = {cxns[1]: onoz}
        expected = succeeded.copy()
        expected.update(failed)
        assert result == expected
        assert result.succeeded == succeeded
        assert result.failed == failed

    @mark.parametrize("method", ALL_METHODS)
    def returns_results_mapping(self, method):
        cxns = [Mock(name=x) for x in ("host1", "host2", "host3")]
        g = ThreadingGroup.from_connections(cxns)
        result = getattr(g, method)("whatever", hide=True)
        assert isinstance(result, GroupResult)
        expected = {x: getattr(x, method).return_value for x in cxns}
        assert result == expected
        assert result.succeeded == expected
        assert result.failed == {}


class ResultManifest_:
    """
    Tests for the explicit, ordered result manifest.

    Each input item keeps a per-input-identity status (success / command
    failure / connection failure / not started), order is input order (not
    completion order), and cancellation only blocks not-started items.
    """

    def _run_in_thread(self, g, method="run", *args, **kwargs):
        """Run a group method in a thread; return result or wrapped result."""
        holder = {"raised": False}

        def target():
            try:
                holder["result"] = getattr(g, method)(*args, **kwargs)
            except GroupException as e:
                holder["raised"] = True
                holder["result"] = e.result

        t = threading.Thread(target=target)
        t.start()
        return t, holder

    def manifest_preserves_input_order_despite_reverse_completion(self):
        # Three fake connections complete in reverse order; the manifest must
        # still be in input order.
        cxns = [Mock(host=x) for x in ("host1", "host2", "host3")]
        item_events = [threading.Event() for _ in cxns]
        ready_events = [threading.Event() for _ in cxns]

        def make_side_effect(i):
            def side_effect(*a, **k):
                ready_events[i].set()
                item_events[i].wait(timeout=5)
                return cxns[i].run.return_value

            return side_effect

        for i, cxn in enumerate(cxns):
            cxn.run = Mock(side_effect=make_side_effect(i))

        g = ThreadingGroup.from_connections(cxns)
        t, holder = self._run_in_thread(g, "run", "whatever", hide=True)
        for ready in ready_events:
            assert ready.wait(timeout=5)
        # Complete in reverse order: 2, 1, 0.
        for i in reversed(range(len(cxns))):
            item_events[i].set()
            time.sleep(0.02)
        t.join(timeout=5)

        result = holder["result"]
        assert [e.connection for e in result.entries] == cxns
        assert result.statuses == ["success", "success", "success"]
        assert result.succeeded == {x: x.run.return_value for x in cxns}
        assert result.failed == {}

    def manifest_records_command_and_connection_failures_in_place(self):
        # One success, one command failure, one connection failure; completing
        # in reverse must land each status in its original position.
        from invoke.exceptions import UnexpectedExit

        cxns = [Mock(host=x) for x in ("host1", "host2", "host3")]
        item_events = [threading.Event() for _ in cxns]
        ready_events = [threading.Event() for _ in cxns]

        success_value = Mock(exited=0)
        command_failure = UnexpectedExit(Mock(exited=1))
        connection_failure = socket.gaierror("boom")

        outcomes = [
            ("success", success_value),
            ("command_failed", command_failure),
            ("connection_failed", connection_failure),
        ]

        def make_side_effect(i):
            def side_effect(*a, **k):
                ready_events[i].set()
                item_events[i].wait(timeout=5)
                status, payload = outcomes[i]
                if status == "success":
                    return payload
                raise payload

            return side_effect

        for i, cxn in enumerate(cxns):
            cxn.run = Mock(side_effect=make_side_effect(i))

        g = ThreadingGroup.from_connections(cxns)
        t, holder = self._run_in_thread(g, "run", "whatever", hide=True)
        for ready in ready_events:
            assert ready.wait(timeout=5)
        for i in reversed(range(len(cxns))):
            item_events[i].set()
            time.sleep(0.02)
        t.join(timeout=5)

        result = holder["result"]
        assert [e.connection for e in result.entries] == cxns
        assert result.statuses == [
            "success",
            "command_failed",
            "connection_failed",
        ]
        assert result.succeeded == {cxns[0]: success_value}
        assert result.failed == {
            cxns[1]: command_failure,
            cxns[2]: connection_failure,
        }
        # A partial result must not be returned as full success.
        assert not result.complete

    def duplicate_aliases_are_separate_entries(self):
        # Two aliases for the same address compare equal but are distinct input
        # items, so the manifest must keep both.
        cxn0 = Connection("samehost")
        cxn1 = Connection("samehost")
        assert cxn0 == cxn1  # same identity (host/user/port)
        assert cxn0 is not cxn1
        cxn0.run = Mock(return_value=Mock(exited=0))
        cxn1.run = Mock(return_value=Mock(exited=0))

        g = ThreadingGroup.from_connections([cxn0, cxn1])
        result = g.run("whatever", hide=True)
        entries = result.entries
        assert len(entries) == 2
        assert entries[0].connection is cxn0
        assert entries[1].connection is cxn1
        assert result.statuses == ["success", "success"]

    def cancellation_marks_queued_items_not_started(self):
        # With a single worker, item 0 runs while 1 and 2 stay queued. After
        # cancel, the queued items are NOT_STARTED; item 0 keeps its real
        # outcome.
        cxns = [Mock(host=x) for x in ("host1", "host2", "host3")]
        item0_started = threading.Event()
        release_item0 = threading.Event()

        def item0_side_effect(*a, **k):
            item0_started.set()
            release_item0.wait(timeout=5)
            return cxns[0].run.return_value

        cxns[0].run = Mock(side_effect=item0_side_effect)

        g = ThreadingGroup.from_connections(cxns, max_workers=1)
        t, holder = self._run_in_thread(g, "run", "whatever", hide=True)
        assert item0_started.wait(timeout=5)
        g.cancel()
        release_item0.set()
        t.join(timeout=5)

        result = holder["result"]
        statuses = result.statuses
        assert statuses[0] == "success"
        assert statuses[1] == "not_started"
        assert statuses[2] == "not_started"
        assert result.not_started
        assert len(result.not_started) == 2
        # The not-started items have no result/exception bodies.
        for entry in result.not_started:
            assert entry.result is None
            assert entry.exception is None

    def cancellation_does_not_block_running_items(self):
        # Cancelling while an item is running must not prevent that item from
        # completing normally.
        cxns = [Mock(host=x) for x in ("host1", "host2")]
        item0_started = threading.Event()
        release_item0 = threading.Event()

        def item0_side_effect(*a, **k):
            item0_started.set()
            release_item0.wait(timeout=5)
            return cxns[0].run.return_value

        cxns[0].run = Mock(side_effect=item0_side_effect)

        g = ThreadingGroup.from_connections(cxns, max_workers=1)
        t, holder = self._run_in_thread(g, "run", "whatever", hide=True)
        assert item0_started.wait(timeout=5)
        g.cancel()
        # Item 0 is still running; give it a moment to finish after release.
        release_item0.set()
        t.join(timeout=5)

        result = holder["result"]
        assert result.statuses[0] == "success"
        assert result.statuses[1] == "not_started"

    def partial_result_with_not_started_raises_group_exception(self):
        # A run that ends with not-started items is partial and must be raised
        # as a GroupException, not returned as if fully successful.
        cxns = [Mock(host=x) for x in ("host1", "host2")]
        item0_started = threading.Event()
        release_item0 = threading.Event()

        def item0_side_effect(*a, **k):
            item0_started.set()
            release_item0.wait(timeout=5)
            return cxns[0].run.return_value

        cxns[0].run = Mock(side_effect=item0_side_effect)

        g = ThreadingGroup.from_connections(cxns, max_workers=1)
        t, holder = self._run_in_thread(g, "run", "whatever", hide=True)
        assert item0_started.wait(timeout=5)
        g.cancel()
        release_item0.set()
        t.join(timeout=5)

        # The partial run must have been raised as a GroupException, not
        # returned as if fully successful.
        assert holder["raised"]
        result = holder["result"]
        assert not result.complete
        assert result.statuses == ["success", "not_started"]

    def repr_does_not_leak_secrets(self):
        # The manifest repr must not expose passwords, full environments or
        # connection parameters.
        from invoke.runners import Result

        cxn = Mock(host="host1")
        secret_result = Result(
            stdout="output",
            env={"PASSWORD": "hunter2", "API_KEY": "secret"},
            exited=0,
        )
        cxn.run = Mock(return_value=secret_result)

        g = ThreadingGroup.from_connections([cxn])
        result = g.run("whatever", hide=True)
        text = repr(result)
        assert "host1" in text
        assert "success" in text
        # Secrets must not appear in the manifest report.
        assert "hunter2" not in text
        assert "secret" not in text
        assert "PASSWORD" not in text
        assert "API_KEY" not in text
        # The result body itself is not embedded.
        assert repr(secret_result) not in text

    def classify_distinguishes_command_and_connection_failures(self):
        from invoke.exceptions import UnexpectedExit

        assert _classify(UnexpectedExit(Mock(exited=1))) == "command_failed"
        assert _classify(socket.gaierror("boom")) == "connection_failed"
        assert _classify(RuntimeError("nope")) == "connection_failed"

    def serial_group_also_populates_manifest_in_order(self):
        cxns = [Mock(host=x) for x in ("host1", "host2", "host3")]
        g = SerialGroup.from_connections(cxns)
        result = g.run("whatever", hide=True)
        assert [e.connection for e in result.entries] == cxns
        assert result.statuses == ["success", "success", "success"]
        assert result.complete

    def empty_group_returns_complete_empty_manifest(self):
        g = ThreadingGroup()
        result = g.run("whatever", hide=True)
        assert isinstance(result, GroupResult)
        assert result.entries == []
        assert result.complete
        assert result.statuses == []
