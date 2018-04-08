Cuteborg
########

Third-party scheduler user interface for borgbackup.

Features:

* Robust architecture (UI cannot crash scheduler, UI monitors scheduler).
* Backups to remote and local repositories.
* Support for repositories on removable and LUKS encrypted devices (mounts
  automatically, handles absence gracefully).
* Schedule based on custom regular intervals
* Qt-based system tray icon with schedule overview
* CLI interface to control and monitor scheduler


Usage
=====

Start scheduler (in foreground; add ``-d`` to daemonize)::

    cuteborg-scheduler -vvv run

Check status::

    cuteborg-scheduler status

Stop scheduler (interrupts jobs which are currently running; borg can cope with
that; they will be re-started on the next scheduler invocation)::

    cuteborg-scheduler stop

Start system tray icon (starts scheduler if not running)::

    qtborg-trayicon
