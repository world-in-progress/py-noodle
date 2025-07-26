# from src.pynoodle.treeger.crm import Treeger
from src.pynoodle.scenario import Scenario

if __name__ == '__main__':
    context = Scenario()
    for name, scenario_node in context.graph.items():
        print(f'Node Name: {name}')
        print(f'Namespace: {scenario_node.namespace}')
        print(f'ICRM Class: {scenario_node.icrm_class}')
        print(f'CRM Class: {scenario_node.crm_class}')