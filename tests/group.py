import socket
import threading
import time
from unittest.mock import Mock, patch, call
from pytest import mark, raises

from invoke.exceptions import UnexpectedExit
from invoke.runners import Result

from fabric import (
    Config,
    Connection,
    Group,
    SerialGroup,
    ThreadingGroup,
    GroupResult,
    GroupManifest,
    HostResult,
)
from fabric.group import thread_worker
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
    def setup(self):
        self.cxns = [Connection(x) for x in ("host1", "host2", "host3")]

    @mark.parametrize("method", ALL_METHODS)
    @patch("fabric.group.Queue")
    @patch("fabric.group.ExceptionHandlingThread")
    def executes_arguments_on_contents_run_via_threading(
        self, Thread, Queue, method
    ):
        queue = Queue.return_value
        g = ThreadingGroup.from_connections(self.cxns)
        # Make sure .exception() doesn't yield truthy Mocks. Otherwise we
        # end up with 'exceptions' that cause errors due to all being the
        # same.
        Thread.return_value.exception.return_value = None
        args = ARGS_BY_METHOD[method]
        kwargs = KWARGS_BY_METHOD[method]
        getattr(g, method)(*args, **kwargs)
        # Testing that threads were used the way we expect is mediocre but
        # I honestly can't think of another good way to assert "threading
        # was used & concurrency occurred"...
        instantiations = [
            call(
                target=thread_worker,
                kwargs=dict(
                    cxn=cxn,
                    queue=queue,
                    method=method,
                    args=args,
                    kwargs=kwargs,
                ),
            )
            for cxn in self.cxns
        ]
        Thread.assert_has_calls(instantiations, any_order=True)
        # These ought to work as by default a Mock.return_value is a
        # singleton mock object
        expected = len(self.cxns)
        for name, got in (
            ("start", Thread.return_value.start.call_count),
            ("join", Thread.return_value.join.call_count),
        ):
            err = "Expected {} calls to ExceptionHandlingThread.{}, got {}"  # noqa
            err = err.format(expected, name, got)
            assert expected, got == err

    @mark.parametrize("method", ALL_METHODS)
    @patch("fabric.group.Queue")
    def queue_used_to_return_results(self, Queue, method):
        # Regular, explicit, mocks for Connections
        cxns = [Mock(host=x) for x in ("host1", "host2", "host3")]
        # Set up Queue with enough behavior to work / assert
        queue = Queue.return_value
        # Ending w/ a True will terminate a while-not-empty loop
        queue.empty.side_effect = (False, False, False, True)
        fakes = [(x, getattr(x, method).return_value) for x in cxns]
        queue.get.side_effect = fakes[:]
        # Execute & inspect results
        g = ThreadingGroup.from_connections(cxns)
        results = getattr(g, method)(
            *ARGS_BY_METHOD[method], **KWARGS_BY_METHOD[method]
        )
        expected = {x: getattr(x, method).return_value for x in cxns}
        assert results == expected
        # Make sure queue was used as expected within worker &
        # ThreadingGroup.run()
        puts = [call(x) for x in fakes]
        queue.put.assert_has_calls(puts, any_order=True)
        assert queue.empty.called
        gets = [call(block=False) for _ in cxns]
        queue.get.assert_has_calls(gets)

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


class RunManifest_:
    def base_group_is_not_implemented(self):
        with raises(NotImplementedError):
            Group("host1").run_manifest("command")

    def returns_manifest_not_exception_for_partial_failures(self):
        cxns = [Mock(host=x) for x in ("host1", "host2")]
        cxns[1].run.side_effect = UnexpectedExit(
            Result(command="nope", exited=1)
        )
        g = SerialGroup.from_connections(cxns)
        # Partial failure must not raise GroupException nor pretend to be
        # a fully successful result; it returns an inspectable manifest.
        manifest = g.run_manifest("command")
        assert isinstance(manifest, GroupManifest)
        assert not manifest.ok
        assert len(manifest.succeeded) == 1
        assert len(manifest.failed) == 1
        assert len(manifest.not_started) == 0
        assert manifest[0].status == HostResult.SUCCESS
        assert manifest[0].result is cxns[0].run.return_value
        assert manifest[1].status == HostResult.COMMAND_FAILED
        assert isinstance(manifest[1].exception, UnexpectedExit)

    def connection_errors_classified_as_connection_failed(self):
        cxns = [Mock(host=x) for x in ("host1", "host2")]
        cxns[0].run.side_effect = socket.gaierror("no such host")
        g = SerialGroup.from_connections(cxns)
        manifest = g.run_manifest("command")
        assert manifest[0].status == HostResult.CONNECTION_FAILED
        assert isinstance(manifest[0].exception, socket.gaierror)
        assert manifest[1].status == HostResult.SUCCESS
        assert not manifest.ok

    def aliases_to_same_address_are_distinct_input_entries(self):
        # Two connections resolving to the same address are still two
        # distinct input items, keyed by position, not by connection.
        cxns = [Mock(host="same"), Mock(host="same")]
        g = SerialGroup.from_connections(cxns)
        manifest = g.run_manifest("command")
        assert len(manifest) == 2
        assert manifest[0] is not manifest[1]
        assert [e.index for e in manifest] == [0, 1]
        assert [e.host for e in manifest] == ["same", "same"]
        assert all(e.status == HostResult.SUCCESS for e in manifest)

    def cancel_only_blocks_not_started_items(self):
        event = threading.Event()
        cxns = [Mock(host=x) for x in ("host1", "host2", "host3")]

        def run_and_cancel(*args, **kwargs):
            event.set()
            return "finished"

        cxns[0].run.side_effect = run_and_cancel
        g = SerialGroup.from_connections(cxns)
        manifest = g.run_manifest("command", cancel_event=event)
        # Already-started item completed normally...
        assert manifest[0].status == HostResult.SUCCESS
        assert manifest[0].result == "finished"
        # ...queued items were never started.
        assert manifest[1].status == HostResult.NOT_STARTED
        assert manifest[2].status == HostResult.NOT_STARTED
        assert not cxns[1].run.called
        assert not cxns[2].run.called
        assert len(manifest.not_started) == 2
        assert not manifest.ok

    def threading_cancel_presets_queued_items_not_started(self):
        event = threading.Event()
        event.set()
        cxns = [Mock(host=x) for x in ("host1", "host2", "host3")]
        g = ThreadingGroup.from_connections(cxns)
        manifest = g.run_manifest("command", cancel_event=event)
        assert [e.status for e in manifest] == [HostResult.NOT_STARTED] * 3
        for cxn in cxns:
            assert not cxn.run.called
        assert len(manifest.not_started) == 3
        assert not manifest.ok

    def threading_reverse_completion_order_lands_in_input_positions(self):
        # Three fake connections whose workers finish in reverse order
        # (host3 first, host1 last); manifest order must follow input
        # order, not completion order.
        cxns = [Mock(host="host%d" % i) for i in range(3)]
        sentinel = Mock(name="slow-result")
        command_failure = UnexpectedExit(Result(command="nope", exited=1))
        connection_failure = socket.gaierror("no such host")

        def slow_success(*args, **kwargs):
            time.sleep(0.3)
            return sentinel

        def medium_command_failure(*args, **kwargs):
            time.sleep(0.15)
            raise command_failure

        cxns[0].run.side_effect = slow_success
        cxns[1].run.side_effect = medium_command_failure
        cxns[2].run.side_effect = connection_failure
        g = ThreadingGroup.from_connections(cxns)
        manifest = g.run_manifest("command")
        # Success and both failures landed back in their original slots.
        assert [e.index for e in manifest] == [0, 1, 2]
        assert [e.host for e in manifest] == ["host0", "host1", "host2"]
        assert [e.status for e in manifest] == [
            HostResult.SUCCESS,
            HostResult.COMMAND_FAILED,
            HostResult.CONNECTION_FAILED,
        ]
        assert manifest[0].result is sentinel
        assert manifest[1].exception is command_failure
        assert manifest[2].exception is connection_failure
        assert not manifest.ok
        assert len(manifest.succeeded) == 1
        assert len(manifest.failed) == 2

    def report_and_repr_omit_secrets_and_connection_params(self):
        config = Config(
            overrides={"run": {"env": {"SECRET_TOKEN": "topsecret"}}}
        )
        cxn = Connection(
            "user@host1",
            config=config,
            connect_kwargs={"password": "s3cret-password"},
        )
        cxn.run = Mock(return_value=Result(command="x", exited=0))
        g = SerialGroup.from_connections([cxn])
        manifest = g.run_manifest("x")
        blob = "\n".join(
            [repr(manifest), manifest.report()]
            + [repr(entry) for entry in manifest]
        )
        assert "s3cret-password" not in blob
        assert "topsecret" not in blob
        assert "SECRET_TOKEN" not in blob
        assert "connect_kwargs" not in blob
        # But it does show position, host identity and status.
        assert "host1" in blob
        assert "success" in blob

    def normal_run_interface_unchanged(self):
        cxns = [Mock(host=x) for x in ("host1", "host2")]
        g = SerialGroup.from_connections(cxns)
        result = g.run("command")
        assert isinstance(result, GroupResult)
        assert result == {x: x.run.return_value for x in cxns}
