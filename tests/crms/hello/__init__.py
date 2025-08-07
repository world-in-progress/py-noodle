from pynoodle import RawScenarioNode

from tests.icrms.ihello import IHello
from .hello import Hello

RAW = RawScenarioNode(
    CRM=Hello,
    ICRM=IHello,
)