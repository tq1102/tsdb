import yaml


class R(str):
    pass


class Q(str):
    pass


class G(str):
    pass


def r_constructor(loader, node):
    return R(loader.construct_scalar(node))


def q_constructor(loader, node):
    return Q(loader.construct_scalar(node))


def g_constructor(loader, node):
    return G(loader.construct_scalar(node))


def env_constructor(loader, node):
    """Load !Env tag"""
    import os

    value = str(loader.construct_scalar(node)) # get the string value next to !Env
    return os.environ[value]

def magic_loader():
    loader = yaml.SafeLoader
    loader.add_constructor("!R", r_constructor)
    loader.add_constructor("!Q", q_constructor)
    loader.add_constructor("!G", g_constructor)
    loader.add_constructor("!Env", env_constructor)
    return loader

