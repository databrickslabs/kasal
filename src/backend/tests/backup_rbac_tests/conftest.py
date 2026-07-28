# Ignore collection of everything under backup_rbac_tests (legacy, deprecated
# RBAC suite). It references models and schemas that no longer exist, so
# collecting it fails at import time.
#
# The hook argument is `collection_path: pathlib.Path`. It used to be
# `path: py.path.local`, which pytest 8 raises PytestRemovedIn9Warning for and
# reports as a collection ERROR, not a warning — so this file, whose entire job
# is to suppress collection errors, had become the source of one. It went
# unnoticed until every directory under tests/ was made a package and this tree
# started being visited.
#
# This whole directory is a backup of a suite that is deliberately never run.
# It is a deletion candidate; git history preserves it either way.


def pytest_ignore_collect(collection_path, config):
    return "backup_rbac_tests" in str(collection_path)
