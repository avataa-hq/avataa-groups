from api.api_v1.router import all_routers
from fastapi import FastAPI


def init_routers(app: FastAPI):
    for router in all_routers:
        app.include_router(router=router)
