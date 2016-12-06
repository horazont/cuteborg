import asyncio

import cuteborg.backend
import cuteborg.subprocess_backend


def progress(line):
    print(repr(line))


# 13.62 MB O 13.62 MB C 13.62 MB D 405 N


async def test():
    ctx = cuteborg.backend.Context()

    ctx.progress_callback = progress
    ctx.compression = cuteborg.backend.CompressionMethod.LZMA

    b = cuteborg.subprocess_backend.LocalSubprocessBackend()

    await b.create_archive(
        "/home/horazont/tmp/repositories/fnord",
        "test2",
        ["/home/horazont/Projects/python/aioxmpp"],
        ctx,
    )

    await b.delete_archive(
        "/home/horazont/tmp/repositories/fnord",
        "test2",
        ctx,
    )


loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(test())
finally:
    loop.close()
