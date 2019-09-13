import pkgutil
import sys, inspect
from typing import List


def list_modules_in_package(pkg_name) -> List[str]:
    """
    :param pkg_name: imported package
    :return: list of modules inside the package (modules will be loaded)
    """
    modules = []
    for importer, modname, ispkg in pkgutil.iter_modules(pkg_name.__path__):
        if not ispkg:
            importer.find_module(modname).load_module(modname)
            modules += [modname]
    return modules


def find_class_in_module(module_name: str, class_name: str):
    """
    :param module_name: full class name. F.e. catcher_modules.database.postgres
    :param class_name: class to search for. F.e. postgres
    :return: found class or None
    """
    for name, obj in inspect.getmembers(sys.modules[module_name]):
        if inspect.isclass(obj) and obj.__name__.lower() == class_name:
            return obj
    return None
