from merger.merger import merge

GTFS_FOLDER = '/Users/gijspeters/gtfs'

PREPROCESS_FOLDER = '/Users/gijspeters/gtfs_preprocess'

APPEND_COMMA_FILES = {
    'routes.txt': ('gtfs-nl_metLUD/routes.txt', 3)
}

INSERT_COMMA_FILES = {
    'trips.txt': ('gtfs-nl_metLUD/trips.txt', 3),
    'trips2.txt': ('NL-20140127/trips.txt', 3)
}

def run():
    merge()

if __name__ == '__main__':
    run()
