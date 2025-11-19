def get_parent_key(node_key: str) -> str:
    # Format node_key to ensure it starts with '.'
    if node_key[0] != '.':
        node_key = '.' + node_key
    
    parent_key = '.'.join(node_key.split('.')[:-1])
    return parent_key if parent_key else '.'
