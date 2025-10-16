from typing import TYPE_CHECKING

from .db.accessor import Database

if TYPE_CHECKING:
    from core.app import Application


class Store:
    """
    Storing the necessary objects for the application.
    """

    def __init__(self, app: "Application"):
        from .grpc.accessor import GRPCAccessor
        from .grpc.server import GRPCServer
        from .kafka.confluent_consumer import CKafkaConsumer
        from .kafka.confluent_producer import CKafkaProducer
        from .keycloak.accessor import KeycloakAccessor
        from .keycloak.token_manager import TokenManager
        from .redis.accessor import RedisAccessor

        self.group_scheme: dict = {}
        # self.grouped_elements: set[int] = set()
        self.grpc = GRPCAccessor(app=app)
        self.grpc_server = GRPCServer(app=app)
        self._keycloak = KeycloakAccessor(app=app)
        self.tokenmanager = TokenManager(keycloak_service=self._keycloak)
        self.kafka = CKafkaConsumer(
            app=app, token_callback=self.tokenmanager.get_token_callback
        )
        self.kafka_prod = CKafkaProducer(
            app=app, token_callback=self.tokenmanager.get_token_callback
        )
        self.redis = RedisAccessor(app=app)


def setup_store(app: "Application") -> None:
    """
    Initial configuration for storage.
    :param app: application instance
    :return:
    """
    app.database = Database(app)
    app.on_startup.append(app.database.connect)
    app.on_shutdown.append(app.database.disconnect)

    app.store = Store(app)
