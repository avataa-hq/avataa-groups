from logging import getLogger
from typing import TYPE_CHECKING

from base.base_accessor import BaseAccessor
from elasticsearch import AsyncElasticsearch

if TYPE_CHECKING:
    from core.app import Application


class ElasticAccessor(BaseAccessor):
    def __init__(self, app: "Application", *args, **kwargs):
        super().__init__(app, *args, **kwargs)
        self.logger = getLogger("ES_Accessor")
        self.es: AsyncElasticsearch | None = None

    async def connect(self, app: "Application"):
        addr = f"{app.config.elastic.ES_PROTOCOL}://{app.config.elastic.ES_HOST}:{app.config.elastic.ES_PORT}"
        self.es = AsyncElasticsearch(hosts=[addr])
        self.logger.info("started.")

    async def disconnect(self, app: "Application"):
        await self.es.close()
        self.logger.info("stoped.")
        # return super().disconnect(app)

    async def remove_data_from_elastic(self, process_instance_key: list[int]):
        PROCESSES_INDEX = "optimize-process-instance-process*"
        query = {
            "bool": {
                "must": [{"terms": {"processInstanceId": process_instance_key}}]
            }
        }
        try:
            response = await self.es.delete_by_query(
                index=PROCESSES_INDEX, body={"query": query}
            )
            return response
        except Exception as e:
            self.logger.error(f"{type(e)}: {e}.")
