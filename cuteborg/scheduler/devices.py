########################################################################
# File name: devices.py
# This file is part of: cuteborg
#
# LICENSE
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
########################################################################
import asyncio
import json
import threading
import pathlib
import pprint
import subprocess

import dbus


_locals = threading.local()


class EncryptedDeviceWithoutPassphrase(Exception):
    pass


@asyncio.coroutine
def lsblk():
    args = [
        "lsblk",
        "--json",
        "-o",
        "NAME,FSTYPE,UUID,MOUNTPOINT,KNAME",
    ]

    proc = yield from asyncio.create_subprocess_exec(
        *args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    stdout, stderr = yield from proc.communicate()
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(
            proc.returncode,
            args,
            stderr=stderr,
        )

    info = json.loads(stdout.decode())

    return info


def lsblk_find_device(devs, uuid):
    for dev in devs:
        if dev["uuid"] == uuid:
            return dev
        try:
            subtree = dev["children"]
        except KeyError:
            pass
        else:
            found_in_subtree = lsblk_find_device(
                subtree,
                uuid,
            )
            if found_in_subtree is not None:
                return found_in_subtree


def _get_threadlocal_bus():
    try:
        return _locals.bus
    except AttributeError:
        _locals.bus = dbus.SystemBus()
        return _locals.bus


def _get_udisks_objects(bus):
    root = bus.get_object(
        "org.freedesktop.UDisks2",
        "/org/freedesktop/UDisks2",
    )

    root_object_manager = dbus.Interface(
        root,
        "org.freedesktop.DBus.ObjectManager",
    )

    return root_object_manager.GetManagedObjects()


def _unlock_device_via_dbus(logger, uuid, crypto_passphrase):
    bus = _get_threadlocal_bus()

    for obj_path in _get_udisks_objects(bus):
        if not str(obj_path).startswith(
                "/org/freedesktop/UDisks2/block_devices/"):
            continue

        dev_obj = bus.get_object(
            "org.freedesktop.UDisks2",
            obj_path,
        )

        dev_props = dbus.Interface(
            dev_obj,
            "org.freedesktop.DBus.Properties",
        )

        dev_uuid = dev_props.Get(
            "org.freedesktop.UDisks2.Block",
            "IdUUID",
        )

        if str(dev_uuid) == uuid:
            dev_encrypted = dbus.Interface(
                dev_obj,
                "org.freedesktop.UDisks2.Encrypted",
            )

            cleartext_obj = bus.get_object(
                "org.freedesktop.UDisks2",
                dev_encrypted.Unlock(crypto_passphrase, []),
            )
            cleartext_dev_props = dbus.Interface(
                cleartext_obj,
                "org.freedesktop.DBus.Properties",
            )

            return bytes(cleartext_dev_props.Get(
                "org.freedesktop.UDisks2.Block",
                "PreferredDevice",
            ))[:-1]  # strip trailing NUL


def _mount_device_via_dbus(logger, uuid):
    bus = _get_threadlocal_bus()

    for obj_path in _get_udisks_objects(bus):
        if not str(obj_path).startswith(
                "/org/freedesktop/UDisks2/block_devices/"):
            continue

        dev_obj = bus.get_object(
            "org.freedesktop.UDisks2",
            obj_path,
        )

        dev_props = dbus.Interface(
            dev_obj,
            "org.freedesktop.DBus.Properties",
        )

        dev_uuid = dev_props.Get(
            "org.freedesktop.UDisks2.Block",
            "IdUUID",
        )

        if str(dev_uuid) == uuid:
            dev_fs = dbus.Interface(
                dev_obj,
                "org.freedesktop.UDisks2.Filesystem",
            )

            return str(dev_fs.Mount([]))


@asyncio.coroutine
def unlock_device_via_dbus(logger, uuid, crypto_passphrase):
    loop = asyncio.get_event_loop()
    return (yield from loop.run_in_executor(
        None,
        _unlock_device_via_dbus,
        logger,
        uuid,
        crypto_passphrase,
    ))


@asyncio.coroutine
def mount_device_via_dbus(logger, uuid):
    loop = asyncio.get_event_loop()
    return (yield from loop.run_in_executor(
        None,
        _mount_device_via_dbus,
        logger,
        uuid,
    ))


@asyncio.coroutine
def wait_for_uuid(logger, uuid, poll_interval):
    p = pathlib.Path("/dev/disk/by-uuid") / uuid
    while True:
        try:
            p = p.resolve()
        except FileNotFoundError:
            pass
        else:
            break

        logger.debug("%s is not present yet ... "
                     "waiting for %.0f seconds",
                     p,
                     poll_interval)
        yield from asyncio.sleep(poll_interval)
    logger.debug("%s appeared", p)


@asyncio.coroutine
def wait_for_mounted(logger, uuid, poll_interval,
                     crypto_passphrase=None):
    while True:
        info = yield from lsblk()
        dev_info = lsblk_find_device(info["blockdevices"], uuid)
        if dev_info is None:
            logger.debug(
                "device %r not there yet ... continuing to wait",
                uuid
            )
            yield from asyncio.sleep(poll_interval)
            continue

        logger.debug(
            "device %r is there, let’s take a look at it",
            uuid
        )

        if dev_info["fstype"] == "crypto_LUKS":
            logger.debug(
                "device %r is encrypted ...",
                uuid,
            )
            if dev_info.get("children", []):
                logger.debug(
                    "but %r appears to be unlocked already :-)",
                    uuid,
                )
                dev_info = dev_info["children"][0]
            else:
                if crypto_passphrase is None:
                    logger.error(
                        "%r is locked, and we do not have a password to try",
                        uuid,
                    )
                    raise EncryptedDeviceWithoutPassphrase(
                        "no passphrase for encrypted device"
                    )

                logger.debug(
                    "%r is locked, and we have a passphrase. "
                    "let’s give it a shot.",
                    uuid,
                )
                yield from unlock_device_via_dbus(
                    logger,
                    dev_info["uuid"],
                    crypto_passphrase,
                )
                logger.debug(
                    "unlocking of %r appears to have worked",
                    uuid,
                )
                # rescan now
                continue

        if dev_info["mountpoint"] is not None:
            logger.debug(
                "device %r is mounted at %r",
                uuid,
                dev_info["mountpoint"]
            )
            return dev_info["mountpoint"]

        logger.debug(
            "attempting to mount %r via dbus",
            uuid,
        )

        return (yield from mount_device_via_dbus(
            logger,
            dev_info["uuid"],
        ))

        yield from asyncio.sleep(poll_interval)
        continue
