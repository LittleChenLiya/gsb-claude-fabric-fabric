import os

from io import BytesIO, StringIO

from unittest.mock import Mock, call, patch
from pytest_relaxed import raises
from pytest import skip  # noqa
import pytest
from paramiko import SFTPAttributes

from fabric import Connection
from fabric.transfer import Transfer


# TODO: pull in all edge/corner case tests from fabric v1


class Transfer_:
    class init:
        "__init__"

        def requires_connection(self):
            # Transfer() -> explodes
            try:
                Transfer()
            except TypeError:
                pass
            else:
                assert False, "Did not raise ArgumentError"
            # Transfer(Connection()) -> happy, exposes an attribute
            cxn = Connection("host")
            assert Transfer(cxn).connection is cxn

    class is_remote_dir:
        def returns_bool_of_stat_ISDIR_flag(self, sftp_objs):
            xfer, sftp = sftp_objs
            # Default mocked st_mode is file-like (first octal digit is 1)
            assert xfer.is_remote_dir("whatever") is False
            # Set mode directory-ish (first octal digit is 4)
            sftp.stat.return_value.st_mode = 0o41777
            assert xfer.is_remote_dir("whatever") is True

        def returns_False_if_stat_raises_IOError(self, sftp_objs):
            xfer, sftp = sftp_objs
            sftp.stat.side_effect = IOError
            assert xfer.is_remote_dir("whatever") is False

    class get:
        class basics:
            def accepts_single_remote_path_posarg(self, sftp_objs):
                transfer, client = sftp_objs
                transfer.get("file")
                client.get.assert_called_with(
                    localpath="/local/file", remotepath="/remote/file"
                )

            def accepts_local_and_remote_kwargs(self, sftp_objs):
                transfer, client = sftp_objs
                transfer.get(remote="path1", local="path2")
                client.get.assert_called_with(
                    remotepath="/remote/path1", localpath="/local/path2"
                )

            def returns_rich_Result_object(self, sftp_objs):
                transfer, client = sftp_objs
                cxn = Connection("host")
                result = Transfer(cxn).get("file")
                assert result.orig_remote == "file"
                assert result.remote == "/remote/file"
                assert result.orig_local is None
                assert result.local == "/local/file"
                assert result.connection is cxn
                # TODO: timing info
                # TODO: bytes-transferred info

        class path_arg_edge_cases:
            def local_None_uses_remote_filename(self, transfer):
                assert transfer.get("file").local == "/local/file"

            def local_empty_string_uses_remote_filename(self, transfer):
                assert transfer.get("file", local="").local == "/local/file"

            @raises(TypeError)
            def remote_arg_is_required(self, transfer):
                transfer.get()

            @raises(ValueError)
            def remote_arg_cannot_be_None(self, transfer):
                transfer.get(None)

            @raises(ValueError)
            def remote_arg_cannot_be_empty_string(self, transfer):
                transfer.get("")

        class local_arg_interpolation:
            def connection_params(self, transfer):
                result = transfer.get("somefile", "{user}@{host}-{port}")
                expected = "/local/{}@host-22".format(transfer.connection.user)
                assert result.local == expected

            def connection_params_as_dir(self, transfer):
                result = transfer.get("somefile", "{host}/")
                assert result.local == "/local/host/somefile"

            def remote_path_posixpath_bits(self, transfer):
                result = transfer.get(
                    "parent/mid/leaf", "foo/{dirname}/bar/{basename}"
                )
                # Recall that test harness sets remote apparent cwd as
                # /remote/, thus dirname is /remote/parent/mid
                assert result.local == "/local/foo/remote/parent/mid/bar/leaf"

        class file_like_local_paths:
            "file-like local paths"

            def _get_to_stringio(self, sftp_objs):
                transfer, client = sftp_objs
                fd = StringIO()
                result = transfer.get("file", local=fd)
                # Note: getfo, not get
                client.getfo.assert_called_with(
                    remotepath="/remote/file", fl=fd
                )
                return result, fd

            def remote_path_to_local_StringIO(self, sftp_objs):
                self._get_to_stringio(sftp_objs)

            def result_contains_fd_for_local_path(self, sftp_objs):
                result, fd = self._get_to_stringio(sftp_objs)
                assert result.remote == "/remote/file"
                assert result.local is fd

        class mode_concerns:
            def setup(self):
                self.attrs = SFTPAttributes()
                self.attrs.st_mode = 0o100644

            def preserves_remote_mode_by_default(self, sftp):
                transfer, client, mock_os = sftp
                # Attributes obj reflecting a realistic 'extended' octal mode
                client.stat.return_value = self.attrs
                transfer.get("file", local="meh")
                # Expect os.chmod to be called with the scrubbed/shifted
                # version of same.
                mock_os.chmod.assert_called_with("/local/meh", 0o644)

            def allows_disabling_remote_mode_preservation(self, sftp):
                transfer, client, mock_os = sftp
                client.stat.return_value = self.attrs
                transfer.get("file", local="meh", preserve_mode=False)
                assert not mock_os.chmod.called

        class local_directory_creation:
            @patch("fabric.transfer.Path")
            def without_trailing_slash_means_leaf_file(self, Path, sftp_objs):
                transfer, client = sftp_objs
                transfer.get(remote="file", local="top/middle/leaf")
                client.get.assert_called_with(
                    localpath="/local/top/middle/leaf",
                    remotepath="/remote/file",
                )
                Path.assert_called_with("top/middle")
                Path.return_value.mkdir.assert_called_with(
                    parents=True, exist_ok=True
                )

            @patch("fabric.transfer.Path")
            def with_trailing_slash_means_mkdir_entire_arg(
                self, Path, sftp_objs
            ):
                transfer, client = sftp_objs
                transfer.get(remote="file", local="top/middle/leaf/")
                client.get.assert_called_with(
                    localpath="/local/top/middle/leaf/file",
                    remotepath="/remote/file",
                )
                Path.assert_called_with("top/middle/leaf/")
                Path.return_value.mkdir.assert_called_with(
                    parents=True, exist_ok=True
                )

    class put:
        class basics:
            def accepts_single_local_path_posarg(self, sftp_objs):
                transfer, client = sftp_objs
                transfer.put("file")
                client.put.assert_called_with(
                    localpath="/local/file", remotepath="/remote/file"
                )

            def accepts_local_and_remote_kwargs(self, sftp_objs):
                transfer, sftp = sftp_objs
                # NOTE: default mock stat is file-ish, so path won't be munged
                transfer.put(local="path2", remote="path1")
                sftp.put.assert_called_with(
                    localpath="/local/path2", remotepath="/remote/path1"
                )

            def returns_rich_Result_object(self, transfer):
                cxn = Connection("host")
                result = Transfer(cxn).put("file")
                assert result.orig_remote is None
                assert result.remote == "/remote/file"
                assert result.orig_local == "file"
                assert result.local == "/local/file"
                assert result.connection is cxn
                # TODO: timing info
                # TODO: bytes-transferred info

        class remote_end_is_directory:
            def appends_local_file_basename(self, sftp_objs):
                xfer, sftp = sftp_objs
                sftp.stat.return_value.st_mode = 0o41777
                xfer.put(local="file.txt", remote="/dir/path/")
                sftp.stat.assert_called_once_with("/dir/path/")
                sftp.put.assert_called_with(
                    localpath="/local/file.txt",
                    remotepath="/dir/path/file.txt",
                )

            class file_like_local_objects:
                def name_attribute_present_appends_like_basename(
                    self, sftp_objs
                ):
                    xfer, sftp = sftp_objs
                    sftp.stat.return_value.st_mode = 0o41777
                    local = StringIO("sup\n")
                    local.name = "sup.txt"
                    xfer.put(local, remote="/dir/path")
                    sftp.putfo.assert_called_with(
                        fl=local, remotepath="/dir/path/sup.txt"
                    )

                @raises(ValueError)
                def no_name_attribute_raises_ValueError(self, sftp_objs):
                    xfer, sftp = sftp_objs
                    sftp.stat.return_value.st_mode = 0o41777
                    local = StringIO("sup\n")
                    xfer.put(local, remote="/dir/path")

        class path_arg_edge_cases:
            def remote_None_uses_local_filename(self, transfer):
                assert transfer.put("file").remote == "/remote/file"

            def remote_empty_string_uses_local_filename(self, transfer):
                assert transfer.put("file", remote="").remote == "/remote/file"

            @raises(ValueError)
            def remote_cant_be_empty_if_local_file_like(self, transfer):
                transfer.put(StringIO())

            @raises(TypeError)
            def local_arg_is_required(self, transfer):
                transfer.put()

            @raises(ValueError)
            def local_arg_cannot_be_None(self, transfer):
                transfer.put(None)

            @raises(ValueError)
            def local_arg_cannot_be_empty_string(self, transfer):
                transfer.put("")

        class file_like_local_paths:
            "file-like local paths"

            def _put_from_stringio(self, sftp_objs):
                transfer, client = sftp_objs
                fd = StringIO()
                result = transfer.put(fd, remote="file")
                # Note: putfo, not put
                client.putfo.assert_called_with(
                    remotepath="/remote/file", fl=fd
                )
                return result, fd

            def remote_path_from_local_StringIO(self, sftp_objs):
                self._put_from_stringio(sftp_objs)

            def local_FLOs_are_rewound_before_putting(self, transfer):
                fd = Mock()
                fd.tell.return_value = 17
                transfer.put(fd, remote="file")
                seek_calls = fd.seek.call_args_list
                assert seek_calls, [call(0) == call(17)]

            def result_contains_fd_for_local_path(self, sftp_objs):
                result, fd = self._put_from_stringio(sftp_objs)
                assert result.remote == "/remote/file"
                assert result.local is fd

        class mode_concerns:
            def preserves_local_mode_by_default(self, sftp):
                transfer, client, mock_os = sftp
                # This is a realistic stat for 0o644
                mock_os.stat.return_value.st_mode = 33188
                transfer.put("file")
                client.chmod.assert_called_with("/remote/file", 0o644)

            def allows_disabling_local_mode_preservation(self, sftp_objs):
                transfer, client = sftp_objs
                transfer.put("file", preserve_mode=False)
                assert not client.chmod.called

        class local_directory_preflight:
            "local directory-source rejection before opening the connection"

            def raises_ValueError_for_local_directory(self, sftp_objs):
                xfer, sftp = sftp_objs
                with patch(
                    "fabric.transfer.os.path.isdir", return_value=True
                ):
                    with pytest.raises(ValueError, match="directory"):
                        xfer.put("somedir", remote="remote-path")
                # No remote SFTP operations of any kind were attempted
                assert sftp.method_calls == []
                sftp.put.assert_not_called()
                sftp.putfo.assert_not_called()
                sftp.stat.assert_not_called()
                sftp.getcwd.assert_not_called()
                sftp.normalize.assert_not_called()
                # And the underlying SSH client never opened an SFTP channel
                xfer.connection.client.open_sftp.assert_not_called()

            def error_message_identifies_offending_source(self, sftp_objs):
                xfer, sftp = sftp_objs
                with patch(
                    "fabric.transfer.os.path.isdir", return_value=True
                ):
                    with pytest.raises(ValueError) as exc:
                        xfer.put("some/dir", remote="remote-path")
                assert "some/dir" in str(exc.value)

            def symlink_to_directory_is_rejected_like_directory(
                self, sftp_objs
            ):
                xfer, sftp = sftp_objs
                # os.path.isdir is what follows symlinks to their final target
                with patch(
                    "fabric.transfer.os.path.isdir",
                    side_effect=lambda path: path == "dir-link",
                ) as isdir:
                    with pytest.raises(ValueError):
                        xfer.put("dir-link", remote="remote-path")
                isdir.assert_called_once_with("dir-link")
                sftp.put.assert_not_called()
                sftp.putfo.assert_not_called()

            def symlink_to_regular_file_is_not_rejected(self, sftp_objs):
                xfer, sftp = sftp_objs
                with patch(
                    "fabric.transfer.os.path.isdir",
                    side_effect=lambda path: False,
                ):
                    xfer.put("file-link", remote="remote-path")
                sftp.put.assert_called_with(
                    localpath="/local/file-link",
                    remotepath="/remote/remote-path",
                )

            def regular_file_source_is_unchanged(self, sftp_objs):
                xfer, sftp = sftp_objs
                # Mocked harness's isdir delegates to the real os.path.isdir,
                # and a nonexistent "file" is therefore not a directory.
                xfer.put("file")
                sftp.put.assert_called_with(
                    localpath="/local/file", remotepath="/remote/file"
                )

            def file_like_sources_always_bypass_preflight(self, sftp_objs):
                xfer, sftp = sftp_objs
                fd = StringIO("data\n")
                with patch("fabric.transfer.os.path.isdir") as isdir:
                    xfer.put(fd, remote="file")
                isdir.assert_not_called()
                sftp.putfo.assert_called_with(
                    fl=fd, remotepath="/remote/file"
                )

            def can_be_disabled_to_restore_legacy_behavior(self, sftp_objs):
                xfer, sftp = sftp_objs
                with patch(
                    "fabric.transfer.os.path.isdir", return_value=True
                ):
                    xfer.put("somedir", remote="file", reject_dirs=False)
                # Remote target resolution & local abspath massage are intact
                sftp.put.assert_called_with(
                    localpath="/local/somedir", remotepath="/remote/file"
                )


# ---------------------------------------------------------------------------
# Real local-fixture acceptance tests (temporary files, directories, symlinks
# and BytesIO) using an in-memory SFTP stand-in. No SSH/SFTP network activity
# can occur: the Connection's memoized _sftp is a Mock, and SSHClient itself
# is patched.
# ---------------------------------------------------------------------------
def _preflight_transfer():
    with patch("fabric.connection.SSHClient") as SSHClient:
        cxn = Connection("host")
        sftp = Mock()
        # Remote "cwd" resolution: no getcwd; normalize gives an anchor
        sftp.getcwd.return_value = None
        sftp.normalize.return_value = "/remote/home"
        # Any remote stat failure means "not a remote directory", matching
        # Transfer.is_remote_dir's existing behavior.
        sftp.stat.side_effect = IOError
        cxn._sftp = sftp  # short-circuit Connection.sftp() memoization
        return Transfer(cxn), sftp, SSHClient


def _link_or_skip(source, link):
    """
    Create a filesystem link at ``link`` pointing at ``source``.

    Uses a real symlink when permitted (POSIX, or privileged/Developer-Mode
    Windows). On Windows without that privilege, falls back to a directory
    junction (for directory targets) or a hard link (for file targets); both
    are still resolved by their final target by ``os.path.isdir``/``isfile``.
    """
    source = str(source)
    link = str(link)
    try:
        os.symlink(source, link)
        return
    except (OSError, NotImplementedError, AttributeError):
        pass
    if os.name != "nt":
        pytest.skip("symlink creation is not permitted on this platform")
    import subprocess

    kind = "/J" if os.path.isdir(source) else "/H"
    try:
        subprocess.run(
            ["cmd", "/c", "mklink", kind, link, source],
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("filesystem link creation is not permitted here")


def test_put_accepts_temporary_file_and_preserves_target_resolution(tmp_path):
    src = tmp_path / "plain.txt"
    src.write_text("payload\n")
    xfer, sftp, _ = _preflight_transfer()
    result = xfer.put(str(src), remote="uploaded.txt")
    sftp.put.assert_called_once_with(
        localpath=os.path.abspath(str(src)),
        remotepath="/remote/home/uploaded.txt",
    )
    assert result.local == os.path.abspath(str(src))
    assert result.remote == "/remote/home/uploaded.txt"
    assert result.orig_local == str(src)


def test_put_rejects_real_directory_without_any_remote_call(tmp_path):
    target_dir = tmp_path / "a-directory"
    target_dir.mkdir()
    src = str(target_dir)
    xfer, sftp, SSHClient = _preflight_transfer()
    with pytest.raises(ValueError) as exc:
        xfer.put(src, remote="remote-target")
    message = str(exc.value)
    assert "directory" in message.lower()
    # {!r} doubles Windows backslashes, so match against repr(src)
    assert repr(src) in message
    # Not a single remote SFTP call...
    assert sftp.method_calls == []
    # ...and the SSH client neither connected nor opened an SFTP channel.
    SSHClient.return_value.connect.assert_not_called()
    SSHClient.return_value.open_sftp.assert_not_called()


def test_put_rejects_directory_symlink_by_final_target(tmp_path):
    target_dir = tmp_path / "actual-directory"
    target_dir.mkdir()
    link = tmp_path / "dir-link"
    _link_or_skip(str(target_dir), str(link))
    assert os.path.isdir(str(link))  # symlink judged via its final target
    xfer, sftp, SSHClient = _preflight_transfer()
    with pytest.raises(ValueError) as exc:
        xfer.put(str(link), remote="remote-target")
    assert repr(str(link)) in str(exc.value)
    assert sftp.method_calls == []
    SSHClient.return_value.open_sftp.assert_not_called()


def test_put_accepts_symlink_pointing_at_regular_file(tmp_path):
    src = tmp_path / "actual.txt"
    src.write_text("via-link\n")
    link = tmp_path / "file-link"
    _link_or_skip(str(src), str(link))
    assert not os.path.isdir(str(link))
    xfer, sftp, _ = _preflight_transfer()
    result = xfer.put(str(link), remote="uploaded.txt")
    sftp.put.assert_called_once_with(
        localpath=os.path.abspath(str(link)),
        remotepath="/remote/home/uploaded.txt",
    )
    assert result.local == os.path.abspath(str(link))


def test_put_accepts_BytesIO_and_rewinds_it(tmp_path):
    fd = BytesIO(b"\x00\x01binary-data")
    xfer, sftp, _ = _preflight_transfer()
    result = xfer.put(fd, remote="blob.bin")
    sftp.putfo.assert_called_once_with(
        fl=fd, remotepath="/remote/home/blob.bin"
    )
    assert sftp.put.called is False
    # File position is restored after the upload
    assert fd.tell() == 0
    assert result.local is fd
    assert result.remote == "/remote/home/blob.bin"
