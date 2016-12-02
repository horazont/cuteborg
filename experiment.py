import cuteborg.backend
import cuteborg.subprocess_backend


def progress(line):
    print(repr(line))

# 13.62 MB O 13.62 MB C 13.62 MB D 405 N scripts.irssi.org...scripts/ignore_log.pl


ctx = cuteborg.backend.Context()
ctx.progress_callback = progress
ctx.compression = cuteborg.backend.CompressionMethod.LZMA

b = cuteborg.subprocess_backend.LocalSubprocessBackend()
b.create_archive(
    "/home/horazont/tmp/repositories/fnord",
    "test2",
    ["/home/horazont/Projects/python/aioxmpp"],
    ctx,
)

b.delete_archive(
    "/home/horazont/tmp/repositories/fnord",
    "test2",
    ctx,
)
