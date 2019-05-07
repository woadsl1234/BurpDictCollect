from lib.config import BLACKEXT, BLACKHOST

# extractor data from host
def filterHost(host):

    blackHosts = BLACKHOST

    for blackHost in blackHosts:
        if host.endswith(blackHost):
            return

    return True

# extractor data from file
def filterFile(file):

    balckPaths = BLACKEXT

    for blackPath in balckPaths:
        if file.endswith(blackPath):
            return False
    return True