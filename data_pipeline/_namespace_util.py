import re

class NamespaceInfo(object):
    """Class that provides namespace information based on a given namespace name.

    Currently we treat namespaces under format detailed in FORMAT ((main|dev)\.)?cluster\.database(\.transformer)*
    Note: we use regex for validation but not processing
    """
    ENVIRONMENTS = ['main', 'dev']
    FORMAT = re.compile(r"^((main|dev)\.)?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)(\.[a-zA-Z0-9_-]+)*$")

    def __init__(self, namespace_name):
        if not self.FORMAT.match(namespace_name):
            raise ValueError("Namespace name not in proper format for info to be extracted")
        sections = namespace_name.split('.')
        if sections[0] in self.ENVIRONMENTS:
            cluster_pos = 1
            self.environment = sections[0]
        else:
            cluster_pos = 0
            self.environment = None
        self.cluster = sections[cluster_pos]
        self.database = sections[cluster_pos+1]
        self.transformers = sections[cluster_pos+2:]

