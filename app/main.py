import asyncio

# import tracemalloc
# from datetime import datetime
from contextlib import asynccontextmanager
from logging import getLogger

from core.app import setup_app
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from infrastructure.router import init_routers
from init_app import create_app

logger = getLogger("memory_tracker")


@asynccontextmanager
async def lifespan(f_app: FastAPI):
    lifespan_app = setup_app()
    # tracemalloc.start()
    # memory_monitor_task = asyncio.create_task(monitor_memory_usage())

    for el in lifespan_app.on_startup:
        await asyncio.create_task(el(lifespan_app))
    yield {"lifespan_app": lifespan_app}

    # memory_monitor_task.cancel()
    # try:
    #     await memory_monitor_task
    # except asyncio.CancelledError:
    #     pass

    for el in lifespan_app.on_shutdown:
        await el(lifespan_app.store)


def init_app():
    app_version = "1"
    app_title = "Group Builder"
    prefix = f"/api/{app_title.replace(' ', '_').lower()}/v{app_version}"
    cur_app = create_app(
        lifespan=lifespan,
        root_path=prefix,
        title=app_title,
        version=app_version,
    )
    cur_app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["*"],
    )

    init_routers(cur_app)
    return cur_app


# IGNORE_PATHS = [
#     "tracemalloc.py",
# ]
#
#
# def should_include_trace(trace):
#     frame = trace[0]
#     filename = frame.filename
#
#     return not any(ignored_path in filename for ignored_path in IGNORE_PATHS)


# async def monitor_memory_usage():
#     tracemalloc.start()
#     previous_snapshot = tracemalloc.take_snapshot()
#     while True:
#         try:
#             await asyncio.sleep(30)
#             current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#             print(f"\n[Memory Usage Analysis at {current_time}]")
#
#             current_snapshot = tracemalloc.take_snapshot()
#
#             stats = current_snapshot.compare_to(previous_snapshot, "lineno")
#             filtered_stats = [
#                 stat for stat in stats if should_include_trace(stat.traceback)
#             ]
#
#             for stat in filtered_stats[:5]:
#                 frame = stat.traceback[0]
#                 logger.info(
#                     f"{frame.filename}:{frame.lineno}: "
#                     f"{stat.size_diff / 1024:,.1f} KB "
#                     f"({stat.count_diff:+d} objects)"
#                 )
#
#             current, peak = tracemalloc.get_traced_memory()
#             logger.info(f"Current memory usage: {current / 1024 / 1024:.2f} MB")
#             logger.info(f"Peak memory usage: {peak / 1024 / 1024:.2f} MB")
#             logger.info("-" * 30)
#
#             previous_snapshot = current_snapshot
#
#         except asyncio.CancelledError:
#             break
#         except Exception as e:
#             print(f"Error in memory monitoring: {e}")
#             await asyncio.sleep(30)


app = init_app()
