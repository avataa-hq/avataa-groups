from fastapi import APIRouter, Request, status

router = APIRouter(prefix="/statistic", tags=["Statistic"])


@router.get("/group_schema", status_code=status.HTTP_200_OK)
async def get_information_about_group_schema(
    request: Request,
) -> dict[str, list[str]]:
    result = request.state.lifespan_app.store.group_scheme
    return {"created group_schema": list(result.keys())}
