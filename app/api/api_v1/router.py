from .endpoints.element import router as router_element
from .endpoints.group import router as router_group
from .endpoints.group_template import router as router_group_template
from .endpoints.information import router as router_information

all_routers = [
    router_element,
    router_group,
    router_group_template,
    router_information,
]
